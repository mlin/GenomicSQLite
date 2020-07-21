/*
 * sam_into_sqlite: load SAM/BAM/CRAM into a GenomicSQLite database.
 * - reference (target) sequences and read groups are loaded from the SQ and RG header lines,
 *   respectively, and stored in dimension tables for reference by integer IDs elsewhere.
 * - a main table holds the alignment details, while QNAME, SEQ, and QUAL reside in a separate,
 *   cross-referenced table. so reader can elect whether to load/decompress the sequences.
 * - tags (written in JSON) also get their own table.
 */
#include "common.hpp"
#include <fstream>
#include <getopt.h>
#include <htslib/sam.h>
#include <map>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std;

// auto-destructing htslib kstring_t
unique_ptr<kstring_t, void (*)(kstring_t *)> kstringXX() {
    return unique_ptr<kstring_t, void (*)(kstring_t *)>(new kstring_t{0, 0, 0}, [](kstring_t *p) {
        free(p->s);
        delete p;
    });
}

// populate readgroups table based on @RG header lines
map<string, int> import_readgroups(const string &table_prefix, sam_hdr_t *hdr, SQLite::Database &db,
                                   bool progress) {
    string ddl =
        "CREATE TABLE " + table_prefix +
        "readgroups(rg_id INTEGER PRIMARY KEY, rg_name TEXT NOT NULL UNIQUE, rg_tags_json TEXT NOT NULL DEFAULT '{}')";
    progress &&cerr << ddl << endl;
    db.exec(ddl);

    SQLite::Statement insert(db, "INSERT INTO " + table_prefix +
                                     "readgroups(rg_id,rg_name,rg_tags_json) VALUES(?,?,?)");

    int nRG = sam_hdr_count_lines(hdr, "RG");
    map<string, int> ans;
    OStringStream tags_json;
    auto ks = kstringXX(), ks_name = kstringXX();
    for (int rg_id = 0; rg_id < nRG; ++rg_id) {
        if (sam_hdr_find_tag_pos(hdr, "RG", rg_id, "ID", ks_name.get())) {
            throw runtime_error("invalid header RG");
        }
        _DBG << ks_name->s << endl;

        tags_json.Clear();
        tags_json << '{';
        bool first = true;
        for (string key :
             {"BC", "CN", "DS", "DT", "FO", "KS", "LB", "PG", "PI", "PL", "PM", "PU", "SM"}) {
            // FIXME: isn't there a way to enumerate the keys actually present?
            if (sam_hdr_find_tag_pos(hdr, "RG", rg_id, key.c_str(), ks.get()) == 0) {
                if (!first)
                    tags_json << ',';
                tags_json << '"' << key << "\":\"" << ks->s << "\"";
                first = false;
            }
        }
        tags_json << '}';

        insert.reset();
        insert.bind(1, rg_id);
        insert.bindNoCopy(2, ks_name->s);
        insert.bindNoCopy(3, tags_json.Get());
        insert.exec();

        ans[string(ks_name->s)] = rg_id;
    }

    return ans;
}

// Given tab-split SAM line, write JSON dict of read's tags (aux) into out; return rg_id or -1
int write_tags_json(const map<string, int> &readgroups, const vector<char *> &sam_fields,
                    OStringStream &out) {
    out << '{';
    int rg_id = -1;
    vector<char *> tag;
    bool first = true;
    for (int c = 11; c < sam_fields.size(); ++c) {
        tag.clear();
        split(sam_fields[c], ':', back_inserter(tag), 2);
        assert(tag.size() == 3);

        if (!strcmp(tag[0], "RG")) {
            string rg = tag[2];
            auto rgp = readgroups.find(rg);
            if (rgp == readgroups.end())
                throw runtime_error("unknown RG: " + rg);
            rg_id = rgp->second;
        } else {
            if (!first)
                out << ',';
            out << '"' << tag[0] << '"' << ':';
            if (!strcmp(tag[1], "i")) {
                out << tag[2];
            } else {
                out << '"' << tag[2] << '"';
            }
            first = false;
        }
    }
    out << '}';
    return rg_id;
}

void help() {
    cout << "sam_into_sqlite: import SAM/BAM/CRAM into GenomicSQLite database" << '\n'
         << GIT_REVISION << "   " << __DATE__ << '\n'
         << '\n'
         << "sam_into_sqlite [options] in.bam|- out.db" << '\n'
         << "Options: " << '\n'
         << "  --table-prefix PREFIX  prefix to the name of each table created" << '\n'
         << "  --no-gri               skip genomic range indexing" << '\n'
         << "  --no-qname-index       skip QNAME indexing" << '\n'
         << "  -l,--level LEVEL       database compression level (-7 to 22, default 6)" << '\n'
         << "  -q,--quiet             suppress progress information on standard error" << '\n'
         << "  -h,--help              show this help message" << '\n'
         << '\n';
}

int main(int argc, char *argv[]) {
    string table_prefix, infilename, outfilename;
    bool gri = true, qname_idx = true, progress = true;
    int level = 6;

    static struct option long_options[] = {{"help", no_argument, 0, 'h'},
                                           {"quiet", no_argument, 0, 'q'},
                                           {"level", required_argument, 0, 'l'},
                                           {"table-prefix", required_argument, 0, 't'},
                                           {"no-gri", no_argument, 0, 'G'},
                                           {"no-qname-index", no_argument, 0, 'Q'},
                                           {0, 0, 0, 0}};

    int c;
    while (-1 != (c = getopt_long(argc, argv, "hqQGl:t:", long_options, nullptr))) {
        switch (c) {
        case 'h':
            help();
            return 0;
        case 'q':
            progress = false;
            break;
        case 'G':
            gri = false;
            break;
        case 'Q':
            qname_idx = false;
            break;
        case 't':
            table_prefix = optarg;
            break;
        case 'l':
            errno = 0;
            level = strtol(optarg, nullptr, 10);
            if (errno || level < -7 || level > 22) {
                cerr << "sam_into_sqlite: couldn't parse --level in [-7,22]";
                return -1;
            }
            break;
        default:
            help();
            return -1;
        }
    }

    if (optind != argc - 2) {
        help();
        return -1;
    }

    infilename = argv[argc - 2];
    outfilename = argv[argc - 1];

    if (infilename == "-" && isatty(STDIN_FILENO)) {
        help();
        return -1;
    }

    // open infilename & read SAM header
    unique_ptr<samFile, void (*)(samFile *)> sam(sam_open(infilename.c_str(), "r"),
                                                 [](samFile *f) { sam_close(f); });
    if (!sam) {
        cerr << "sam_into_sqlite: failed opening " << infilename << endl;
        return 1;
    }
    hts_set_threads(sam.get(), max(2U, thread::hardware_concurrency() / 4));
    unique_ptr<sam_hdr_t, void (*)(sam_hdr_t *)> hdr(sam_hdr_read(sam.get()), &sam_hdr_destroy);
    if (!hdr) {
        cerr << "sam_into_sqlite: failed reading VCF header from " << infilename << '\n';
        return 1;
    }

    try {
        // open output database
        sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);
        sqlite3_config(SQLITE_CONFIG_LOOKASIDE, 2048, 128);
        auto db = GenomicSQLiteOpen(
            outfilename, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX,
            R"( {"unsafe_load": true, "zstd_level": )" + to_string(level) + "}");
#ifndef NDEBUG
        db->exec("PRAGMA foreign_keys=ON");
#endif
        SQLite::Transaction txn(*db);

        // import SQs
        // TODO: allow to open existing db with consistent assembly
        int nSQ = sam_hdr_nref(hdr.get());
        auto ks = kstringXX();
        for (int rid = 0; rid < nSQ; ++rid) {
            if (sam_hdr_find_tag_pos(hdr.get(), "SQ", rid, "SN", ks.get())) {
                throw runtime_error("invalid header SQ");
            }
            string name = ks->s;
            if (sam_hdr_find_tag_pos(hdr.get(), "SQ", rid, "LN", ks.get())) {
                throw runtime_error("invalid header SQ");
            }
            errno = 0;
            sqlite3_int64 length = strtoll(ks->s, nullptr, 10);
            if (errno || length <= 0) {
                throw runtime_error("invalid header SQ");
            }
            string m5;
            if (sam_hdr_find_tag_pos(hdr.get(), "SQ", rid, "M5", ks.get()) == 0) {
                m5 = ks->s;
            }
            string sql = PutGenomicReferenceSequenceSQL(name, length, "", m5, "{}", rid);
            if (rid == 0) {
                progress &&cerr << sql << endl;
            } else if (rid == 1) {
                progress &&cerr << "  ..." << endl;
            }
            db->exec(sql);
        }

        // import readgroups
        auto readgroups = import_readgroups(table_prefix, hdr.get(), *db, progress);

        // formulate & apply DDL
        // TODO: allow --append
        string ddl =
            "CREATE TABLE " + table_prefix +
            "reads(rowid INTEGER PRIMARY KEY, flag INTEGER NOT NULL, rid INTEGER REFERENCES _gri_refseq(_gri_rid), pos INTEGER, endpos INTEGER, "
            "mapq INTEGER, cigar TEXT, rnext INTEGER, pnext INTEGER, tlen INTEGER, "
            "rg_id INTEGER REFERENCES " +
            table_prefix + "readgroups(rg_id))";
        ddl += ";\nCREATE TABLE " + table_prefix +
               "reads_seqs(rowid INTEGER PRIMARY KEY REFERENCES " + table_prefix +
               "reads(rowid), qname TEXT, seq TEXT, qual TEXT)";
        ddl += ";\nCREATE TABLE " + table_prefix +
               "reads_tags(rowid INTEGER PRIMARY KEY REFERENCES " + table_prefix +
               "reads(rowid), tags_json TEXT NOT NULL DEFAULT '{}')";

        progress &&cerr << ddl << endl;
        db->exec(ddl);

        // prepare row insertion statements
        SQLite::Statement insert_read(
            *db,
            "INSERT INTO " + table_prefix +
                "reads(flag,rid,pos,endpos,mapq,cigar,rnext,pnext,tlen,rg_id) VALUES(?,?,?,?,?,?,?,?,?,?)");
        SQLite::Statement insert_seqs(*db, "INSERT INTO " + table_prefix +
                                               "reads_seqs(rowid,qname,seq,qual) VALUES(?,?,?,?)");
        SQLite::Statement insert_tags(*db, "INSERT INTO " + table_prefix +
                                               "reads_tags(rowid,tags_json) VALUES(?,?)");

        // stream bam1_t records
        progress &&cerr << "inserting reads...";
        unique_ptr<bam1_t, void (*)(bam1_t *)> rec(bam_init1(), bam_destroy1);
        OStringStream cigarstr, tagsbuf;
        auto sam_line = kstringXX();
        vector<char *> sam_fields;
        int rc;
        while ((rc = sam_read1(sam.get(), hdr.get(), rec.get())) >= 0) {
            // Load the tab-split SAM line too, as some fields are easier to access that way
            if (sam_format1(hdr.get(), rec.get(), sam_line.get()) < 0) {
                throw runtime_error("corrupt SAM");
            }
            sam_fields.clear();
            split(sam_line->s, '\t', back_inserter(sam_fields));
            assert(sam_fields.size() >= 11);

            insert_read.reset();
            insert_read.clearBindings();

            insert_read.bind(1, rec->core.flag); // flag
            if (rec->core.tid >= 0 && rec->core.tid < nSQ)
                insert_read.bind(2, rec->core.tid); // rid
            if (rec->core.pos >= 0) {
                insert_read.bind(3, rec->core.pos); // pos
                auto endpos = bam_endpos(rec.get());
                if (endpos >= rec->core.pos) {
                    insert_read.bind(4, endpos); // endpos
                }
            }
            insert_read.bind(5, rec->core.qual); // mapq

            char *cigar = sam_fields[5];
            if (*cigar && strcmp(cigar, "*"))
                insert_read.bindNoCopy(6, cigar); // cigar

            if (rec->core.mtid >= 0 && rec->core.mtid <= nSQ)
                insert_read.bind(7, rec->core.mtid); // rnext
            if (rec->core.mpos >= 0)
                insert_read.bind(8, rec->core.mpos); // pnext
            if (rec->core.isize)
                insert_read.bind(9, rec->core.isize); // tlen

            tagsbuf.Clear();
            int rg_id = write_tags_json(readgroups, sam_fields, tagsbuf);
            if (rg_id >= 0 && rg_id < readgroups.size()) {
                insert_read.bind(10, rg_id); // rg_id
            }

            insert_read.exec();
            sqlite3_int64 rowid = db->getLastInsertRowid();

            insert_seqs.reset();
            insert_seqs.clearBindings();
            insert_seqs.bind(1, rowid);
            insert_seqs.bindNoCopy(2, bam_get_qname(rec.get())); // qname
            char *seq = sam_fields[9];
            if (*seq && strcmp(seq, "*"))
                insert_seqs.bindNoCopy(3, seq); // seq
            char *qual = sam_fields[10];
            if (*qual && strcmp("*", qual))
                insert_seqs.bindNoCopy(4, qual); // qual
            insert_seqs.exec();

            insert_tags.reset();
            insert_tags.bind(1, rowid);
            insert_tags.bind(2, tagsbuf.Get()); // tags_json
            insert_tags.exec();
        }
        if (rc != -1) {
            throw std::runtime_error("error reading SAM records");
        }

        // create indices
        if (gri) {
            string gri_sql =
                CreateGenomicRangeIndexSQL(table_prefix + "reads", "rid", "pos", "endpos");
            progress &&cerr << gri_sql << endl;
            db->exec(gri_sql);
        }
        if (qname_idx) {
            string qname_idx_sql = "CREATE INDEX " + table_prefix + "reads_qname ON " +
                                   table_prefix + "reads_seqs(qname)";
            progress &&cerr << qname_idx_sql << endl;
            db->exec(qname_idx_sql);
        }
        progress &&cerr << "COMMIT" << endl;
        txn.commit();
    } catch (exception &exn) {
        cerr << "sam_into_sqlite: " << exn.what() << '\n';
        return 1;
    }

    return 0;
}
