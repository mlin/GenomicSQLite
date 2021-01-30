/*
 * vcf_into_sqlite: load VCF/gVCF/pVCF into a GenomicSQLite database with an exhaustively detailed
 * schema, representing all fields in SQL columns.
 * - if there are individual genotypes:
 *   - they & FORMAT fields go into a separate table keyed by (variant,sample)
 *   - sample names go into a dimension table referred to by integer ID elsewhere
 * - array values are stored as JSON
 */
#include "common.hpp"
#include <cmath>
#include <functional>
#include <getopt.h>
#include <htslib/vcf.h>
#include <map>
#include <set>
#include <sstream>
#include <unistd.h>

// unpack each bcf_hrec_t with the key type (e.g. INFO, FORMAT) into an easier-to-use map
vector<map<string, string>> extract_hrecs(bcf_hdr_t *hdr, const char *key,
                                          const set<string> &required_keys = {}) {
    vector<map<string, string>> ans;
    for (int i = 0; i < hdr->nhrec; ++i) {
        bcf_hrec_t *hrec = hdr->hrec[i];
        if (!strcmp(hrec->key, key)) {
            map<string, string> entry;
            for (int j = 0; j < hrec->nkeys; ++j) {
                entry[string(hrec->keys[j])] = string(hrec->vals[j]);
            }
            for (const auto &k : required_keys) {
                auto p = entry.find(k);
                if (p == entry.end()) {
                    throw runtime_error("VCF header " + string(key) +
                                        " line missing required field: " + k);
                }
            }
            ans.push_back(move(entry));
        }
    }
    return ans;
}

string schemaDDL(const string &table_prefix, vector<map<string, string>> &info_hrecs,
                 vector<map<string, string>> &format_hrecs, int ploidy,
                 bool genotypes_without_rowid) {

    OStringStream out;
    out << "CREATE TABLE " << table_prefix
        << "variants (variant_rowid INTEGER NOT NULL PRIMARY KEY, rid INTEGER NOT NULL REFERENCES _gri_refseq(_gri_rid), "
           "POS INTEGER NOT NULL, rlen INTEGER NOT NULL, ID_jsarray TEXT DEFAULT '[]', REF TEXT NOT NULL, "
           "ALT_jsarray TEXT NOT NULL DEFAULT '[]', QUAL REAL, FILTER_jsarray";

    // INFO columns
    for (auto &hrec : info_hrecs) {
        out << "\n, " << hrec["ID"];
        if (hrec["Type"] == "Flag") {
            out << " INTEGER NOT NULL";
        } else if (hrec["Number"] != "1" &&
                   (hrec["Type"] == "Integer" || hrec["Type"] == "Float")) {
            out << "_jsarray TEXT DEFAULT '[]'";
        } else if (hrec["Type"] == "Integer") {
            out << " INTEGER";
        } else if (hrec["Type"] == "Float") {
            out << " REAL";
        } else {
            out << " TEXT";
        }

        // add SQL comment with original metadata
        out << "  --  "
            << "Number=" << hrec["Number"] << ",Type=" << hrec["Type"];
        auto desc = hrec.find("Description");
        if (desc != hrec.end() && !desc->second.empty()) {
            out << ",Description=" << desc->second;
        }
    }
    out << "\n)";

    if (!format_hrecs.empty()) {
        // TODO: include metadata from SAMPLE header lines
        out << ";\nCREATE TABLE " << table_prefix
            << "samples (sample_id INTEGER NOT NULL PRIMARY KEY, sample_name TEXT NOT NULL)";
        out << ";\nCREATE TABLE " << table_prefix
            << "genotypes (variant_rowid INTEGER NOT NULL REFERENCES " << table_prefix
            << "variants(variant_rowid), sample_id INTEGER NOT NULL REFERENCES " << table_prefix
            << "samples(sample_id)";

        // FORMAT columns
        for (auto &hrec : format_hrecs) {
            if (hrec["ID"] == "GT") {
                for (int i = 1; i <= ploidy; ++i) {
                    out << "\n, GT" << to_string(i) << " INTEGER  --  allele called on homolog "
                        << to_string(i);
                }
                if (ploidy > 1) {
                    out << "\n, GT_ploidy INTEGER  --  number of homologs called, negated if calls are phased";
                }
            } else {
                out << "\n, " << hrec["ID"];
                if (hrec["Number"] != "1" &&
                    (hrec["Type"] == "Integer" || hrec["Type"] == "Float")) {
                    out << "_jsarray TEXT DEFAULT '[]'";
                } else if (hrec["Type"] == "Integer") {
                    out << " INTEGER";
                } else if (hrec["Type"] == "Float") {
                    out << " REAL";
                } else {
                    out << " TEXT";
                }

                // add SQL comment with original metadata
                out << "  --  "
                    << "Number=" << hrec["Number"] << ",Type=" << hrec["Type"];
                auto desc = hrec.find("Description");
                if (desc != hrec.end() && !desc->second.empty()) {
                    out << ",Description=" << desc->second;
                }
            }
        }
        out << "\n, PRIMARY KEY (variant_rowid, sample_id))";
        if (genotypes_without_rowid) {
            out << " WITHOUT ROWID";
        }
    }

    return string(out.Get());
}

unique_ptr<SQLite::Statement> prepare_insert_variant(const string &table_prefix,
                                                     vector<map<string, string>> &info_hrecs,
                                                     SQLite::Database &db) {
    OStringStream sql;
    sql << "INSERT INTO " << table_prefix << "variants VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?";
    for (auto &hrec : info_hrecs) {
        sql << ", ?";
    }
    sql << ")";
    return unique_ptr<SQLite::Statement>(new SQLite::Statement(db, sql.Get()));
}

unique_ptr<SQLite::Statement> prepare_insert_genotype(const string &table_prefix,
                                                      vector<map<string, string>> &format_hrecs,
                                                      int ploidy, SQLite::Database &db) {
    OStringStream sql;
    sql << "INSERT INTO " << table_prefix << "genotypes VALUES(?, ?";
    for (auto &hrec : format_hrecs) {
        sql << ", ?";
        if (hrec["ID"] == "GT") {
            for (int i = 0; i < ploidy - (ploidy > 1 ? 0 : 1); ++i) {
                sql << ", ?";
            }
        }
    }
    sql << ")";
    return unique_ptr<SQLite::Statement>(new SQLite::Statement(db, sql.Get()));
}

void insert_samples(bcf_hdr_t *hdr, const string &table_prefix, SQLite::Database &db) {
    SQLite::Statement stmt(db, "INSERT INTO " + table_prefix +
                                   "samples(sample_id,sample_name) VALUES(?,?)");

    for (sqlite3_int64 i = 0; i < bcf_hdr_nsamples(hdr); ++i) {
        stmt.bind(1, i);
        stmt.bind(2, hdr->samples[i]);
        stmt.exec();
        stmt.reset();
    }
}

void write_int32_jsarray(OStringStream &out, int32_t *v, int n) {
    out << '[';
    for (int i = 0; i < n; ++i) {
        if (v[i] == bcf_int32_vector_end ||
            (v[0] == bcf_int32_missing && (n == 1 || v[1] == bcf_int32_vector_end))) {
            break;
        }
        if (i)
            out << ",";
        if (v[i] != bcf_int32_missing) {
            out << to_string(v[i]);
        } else {
            out << "null";
        }
    }
    out << ']';
}

void write_float_jsarray(OStringStream &out, float *v, int n) {
    out << '[';
    for (int i = 0; i < n; ++i) {
        if (bcf_float_is_vector_end(v[i]) ||
            (bcf_float_is_missing(v[0]) && (n == 1 || bcf_float_is_vector_end(v[1])))) {
            break;
        }
        if (i)
            out << ",";
        if (!bcf_float_is_missing(v[i])) {
            // use sstream due to weird behavior of std::to_string(float)
            ostringstream formatter;
            formatter << v[i];
            out << formatter.str();
        } else {
            out << "null";
        }
    }
    out << ']';
}

// convenience wrapper for a self-freeing vector with an exposed 'capacity' --
// used with htslib functions that reuse/realloc the buffer
template <class T> struct htsvecbox {
    T *v = nullptr;
    int capacity = 0; // in number of elements, NOT bytes
    bool empty() const { return v == nullptr; }
    T &operator[](unsigned i) { return v[i]; }
    void clear() {
        free(v);
        v = nullptr;
        capacity = 0;
    }
    ~htsvecbox() { free(v); }
};

void insert_variant(bcf_hdr_t *hdr, bcf1_t *rec, vector<map<string, string>> &info_hrecs,
                    SQLite::Statement &stmt) {
    stmt.reset();
    stmt.clearBindings();

    // rid, pos, rlen
    stmt.bind(2, rec->rid);
    stmt.bind(3, rec->pos);
    stmt.bind(4, rec->rlen);

    // jsarray of IDs
    string idbuf = rec->d.id;
    vector<char *> ids;
    split(idbuf, ';', back_inserter(ids));
    OStringStream id;
    id << '[';
    bool first = true;
    for (auto id1 : ids) {
        if (*id1 && strcmp(id1, ".")) {
            id << (first ? (first = false, "") : ",") << '"' << id1 << '"';
        }
    }
    id << ']';
    if (id.Size() > 2) {
        stmt.bindNoCopy(5, id.Get());
    }

    // ref allele
    stmt.bindNoCopy(6, rec->d.allele[0]);

    // jsarray of alt alleles
    OStringStream alt;
    alt << '[';
    for (int i = 1; i < rec->n_allele; ++i) {
        alt << (i > 1 ? "," : "") << '"' << rec->d.allele[i] << '"';
    }
    alt << ']';
    if (alt.Size() > 2) {
        stmt.bindNoCopy(7, alt.Get());
    }

    // qual
    if (rec->qual == rec->qual) {
        stmt.bind(8, rec->qual);
    }

    // jsarray of filters
    OStringStream filter;
    filter << '[';
    for (int i = 0; i < rec->d.n_flt; ++i) {
        filter << (i ? "," : "") << '"' << hdr->id[BCF_DT_ID][rec->d.flt[i]].key << '"';
    }
    filter << ']';
    if (filter.Size() > 2) {
        stmt.bindNoCopy(9, filter.Get());
    }

    // INFO columns
    int col = 10;
    htsvecbox<int32_t> int32_box;
    htsvecbox<float> float_box;
    htsvecbox<char> string_box;
    OStringStream buf;
    for (auto &hrec : info_hrecs) {
        const char *id = hrec["ID"].c_str();
        if (hrec["Type"] == "Flag") {
            stmt.bind(col, bcf_get_info_flag(hdr, rec, id, &int32_box.v, &int32_box.capacity) == 1);
        } else if (hrec["Number"] != "1" && // vector as jsarray
                   (hrec["Type"] == "Integer" || hrec["Type"] == "Float")) {
            bool isint = hrec["Type"] == "Integer";
            if (isint) {
                int n = bcf_get_info_int32(hdr, rec, id, &int32_box.v, &int32_box.capacity);
                write_int32_jsarray(buf, int32_box.v, n);
            } else {
                int n = bcf_get_info_float(hdr, rec, id, &float_box.v, &float_box.capacity);
                write_float_jsarray(buf, float_box.v, n);
            }
            if (buf.Size() > 2) {
                stmt.bind(col, buf.Get());
            }
            buf.Clear();
        } else if (hrec["Type"] == "Integer") {
            if (bcf_get_info_int32(hdr, rec, id, &int32_box.v, &int32_box.capacity) == 1 &&
                int32_box[0] != bcf_int32_missing && int32_box[0] != bcf_int32_vector_end) {
                stmt.bind(col, int32_box[0]);
            }
        } else if (hrec["Type"] == "Float") {
            if (bcf_get_info_float(hdr, rec, id, &float_box.v, &float_box.capacity) == 1 &&
                !bcf_float_is_missing(float_box[0]) && !bcf_float_is_vector_end(float_box[0])) {
                stmt.bind(col, float_box[0]);
            }
        } else if (bcf_get_info_string(hdr, rec, id, &string_box.v, &string_box.capacity) > 0) {
            stmt.bind(col, string_box.v);
        }
        ++col;
    }

    stmt.exec();
}

// helper to extract FORMAT values & bind them in insert genotypes statement
class format_helper {
  protected:
    int n_, // total # items
        k_; // items per sample

  public:
    virtual ~format_helper() = default;
    virtual int bind(int sample, SQLite::Statement &stmt, int col) = 0;

    static unique_ptr<format_helper> make(bcf_hdr_t *hdr, bcf1_t *rec, map<string, string> &hrec,
                                          int ploidy);
};

class format_helper_int32 : public format_helper {
  protected:
    htsvecbox<int32_t> box_;
    OStringStream buf_;

  public:
    format_helper_int32(bcf_hdr_t *hdr, bcf1_t *rec, const string &tag) {
        n_ = bcf_get_format_int32(hdr, rec, tag.c_str(), &box_.v, &box_.capacity);
    }

    int bind(int sample, SQLite::Statement &stmt, int col) override {
        assert(sample * k_ + k_ <= n_);
        if (k_ > 1) {
            buf_.Clear();
            write_int32_jsarray(buf_, box_.v + (sample * k_), k_);
            if (buf_.Size() > 2) {
                stmt.bindNoCopy(col, buf_.Get());
            }
        } else {
            int32_t x = box_[sample];
            if (x != bcf_int32_missing && x != bcf_int32_vector_end) {
                stmt.bind(col, x);
            }
        }
        return 1;
    }
};

class format_helper_float : public format_helper {
  protected:
    htsvecbox<float> box_;
    OStringStream buf_;

  public:
    format_helper_float(bcf_hdr_t *hdr, bcf1_t *rec, const string &tag) {
        n_ = bcf_get_format_float(hdr, rec, tag.c_str(), &box_.v, &box_.capacity);
    }

    int bind(int sample, SQLite::Statement &stmt, int col) override {
        assert(sample * k_ + k_ <= n_);
        if (k_ > 1) {
            buf_.Clear();
            write_float_jsarray(buf_, box_.v + (sample * k_), k_);
            if (buf_.Size() > 2) {
                stmt.bindNoCopy(col, buf_.Get());
            }
        } else {
            auto x = box_[sample];
            if (!bcf_float_is_missing(x) && !bcf_float_is_vector_end(x)) {
                stmt.bind(col, x);
            }
        }
        return 1;
    }
};

class format_helper_string : public format_helper {
    htsvecbox<char> box_;
    string buf_;

  public:
    format_helper_string(bcf_hdr_t *hdr, bcf1_t *rec, const string &tag) {
        n_ = bcf_get_format_char(hdr, rec, tag.c_str(), &box_.v, &box_.capacity);
    }

    int bind(int sample, SQLite::Statement &stmt, int col) override {
        char *s = box_.v + sample * k_;
        if (*s && strcmp(s, ".")) {
            buf_.assign(s, strnlen(s, k_));
            stmt.bindNoCopy(col, buf_);
        }
        return 1;
    }
};

class format_helper_GT : public format_helper {
    int ploidy_;
    htsvecbox<int32_t> gt_;

  public:
    format_helper_GT(bcf_hdr_t *hdr, bcf1_t *rec, int ploidy) : ploidy_(ploidy) {
        n_ = bcf_get_genotypes(hdr, rec, &gt_.v, &gt_.capacity);
    }

    int bind(int sample, SQLite::Statement &stmt, int col) override {
        if (k_ > ploidy_) {
            throw runtime_error("set --ploidy >= " + to_string(k_));
        }
        bool ended = false;
        for (int i = 0; i < ploidy_; ++i) {
            if (!ended && i < k_) {
                int call = gt_[sample * k_ + i];
                ended = (call == bcf_int32_vector_end);
                if (!ended && call != bcf_int32_missing && !bcf_gt_is_missing(call)) {
                    stmt.bind(col + i, bcf_gt_allele(call));
                }
            }
        }
        if (ploidy_ > 1) {
            if (k_ > 1 && gt_[sample * k_] != bcf_int32_vector_end &&
                gt_[sample * k_ + 1] != bcf_int32_vector_end &&
                gt_[sample * k_ + 1] != bcf_int32_missing &&
                bcf_gt_is_phased(gt_[sample * k_ + 1])) {
                stmt.bind(col + ploidy_, 0 - k_);
            } else {
                stmt.bind(col + ploidy_, k_);
            }
        }
        return ploidy_ + (ploidy_ > 1 ? 1 : 0);
    }
};

class format_helper_null : public format_helper {
    int cols_;

  public:
    format_helper_null(int cols) : cols_(cols) {}

    int bind(int sample, SQLite::Statement &stmt, int col) override { return cols_; }
};

unique_ptr<format_helper> format_helper::make(bcf_hdr_t *hdr, bcf1_t *rec,
                                              map<string, string> &hrec, int ploidy) {
    unique_ptr<format_helper> ans;
    if (hrec["ID"] == "GT") {
        ans.reset(new format_helper_GT(hdr, rec, ploidy));
    } else if (hrec["Type"] == "Integer") {
        ans.reset(new format_helper_int32(hdr, rec, hrec["ID"]));
    } else if (hrec["Type"] == "Float") {
        ans.reset(new format_helper_float(hdr, rec, hrec["ID"]));
    } else if (hrec["Type"] == "String" || hrec["Type"] == "Character") {
        ans.reset(new format_helper_string(hdr, rec, hrec["ID"]));
    } else {
        throw runtime_error("unknown FORMAT field Type=" + hrec["Type"]);
    }
    if (ans->n_ > 0) {
        auto nsamples = bcf_hdr_nsamples(hdr);
        if (ans->n_ < nsamples || ans->n_ % nsamples) {
            throw runtime_error("sample count doesn't evenly divide vector length; field ID=" +
                                hrec["ID"]);
        }
        ans->k_ = ans->n_ / nsamples;
    } else {
        int null_cols = 1;
        if (hrec["ID"] == "GT") {
            null_cols = ploidy + (ploidy > 1 ? 1 : 0);
        }
        ans.reset(new format_helper_null(null_cols));
    }
    return ans;
}

void insert_genotypes(bcf_hdr_t *hdr, bcf1_t *rec, vector<map<string, string>> &format_hrecs,
                      int ploidy, sqlite3_int64 variant_rowid, SQLite::Statement &stmt) {
    vector<unique_ptr<format_helper>> format_helpers;
    for (auto &hrec : format_hrecs) {
        format_helpers.push_back(format_helper::make(hdr, rec, hrec, ploidy));
    }
    for (sqlite3_int64 sample = 0; sample < bcf_hdr_nsamples(hdr); ++sample) {
        stmt.reset();
        stmt.clearBindings();
        stmt.bind(1, variant_rowid);
        stmt.bind(2, sample);

        int col = 3;
        for (auto &fmt : format_helpers) {
            col += fmt->bind(sample, stmt, col);
        }

        stmt.exec();
    }
}

// stream BCF records using background thread
class BCFReader : public BackgroundProducer<shared_ptr<bcf1_t>> {
  public:
    BCFReader(vcfFile *vcf, bcf_hdr_t *hdr, int ringsize)
        : BackgroundProducer<shared_ptr<bcf1_t>>(ringsize), vcf_(vcf), hdr_(hdr) {}

  private:
    vcfFile *vcf_;
    bcf_hdr_t *hdr_;

    bool Produce(shared_ptr<bcf1_t> &it) override {
        if (!it) {
            it = shared_ptr<bcf1_t>(bcf_init(), &bcf_destroy);
        }
        int ret = bcf_read(vcf_, hdr_, it.get());
        if (ret == -1) {
            return false;
        } else if (ret != 0 || it->errcode) {
            ostringstream msg;
            msg << "VCF parser error: bcf1_read() -> " << ret
                << ", bcf1_t::errcode = " << it->errcode;
            throw runtime_error(msg.str());
        }
        ret = bcf_unpack(it.get(), BCF_UN_ALL);
        if (ret != 0) {
            throw runtime_error("Corrupt VCF/BCF record; bcf_unpack() -> " + to_string(ret));
        }
        return true;
    }
};

void help() {
    cout
        << "vcf_into_sqlite: import .vcf, .vcf.gz, or .bcf into GenomicSQLite database with all fields unpacked"
        << '\n'
        << GIT_REVISION << "   " << __DATE__ << '\n'
        << '\n'
        << "vcf_into_sqlite [options] in.vcf|- out.db" << '\n'
        << "Options: " << '\n'
        << "  --table-prefix PREFIX      prefix to the name of each table created" << '\n'
        << "  --ploidy N                 set max ploidy => # GT columns (default 2)" << '\n'
        << "  --genotypes-without-rowid  make the genotypes table WITHOUT ROWID (advantageous if the FORMAT fields aren't too large)"
        << '\n'
        << "  --no-gri                   skip genomic range indexing" << '\n'
        << "  --inner-page-KiB N         inner page size; one of {1,2,4,8,16,32,64}" << '\n'
        << "  --outer-page-KiB N         outer page size; one of {1,2,4,8,16,32,64}" << '\n'
        << "  -l,--level LEVEL           database compression level (-7 to 22, default 6)" << '\n'
        << "  -q,--quiet                 suppress progress information on standard error" << '\n'
        << "  -h,--help                  show this help message" << '\n'
        << '\n';
}

int main(int argc, char *argv[]) {
    string table_prefix, infilename, outfilename;
    bool gri = true, progress = true, genotypes_without_rowid = false;
    int level = 6, ploidy = 2, inner_page_KiB = 16, outer_page_KiB = 32;

    static struct option long_options[] = {{"help", no_argument, 0, 'h'},
                                           {"quiet", no_argument, 0, 'q'},
                                           {"inner-page-KiB", required_argument, 0, 'I'},
                                           {"outer-page-KiB", required_argument, 0, 'O'},
                                           {"level", required_argument, 0, 'l'},
                                           {"ploidy", required_argument, 0, 'p'},
                                           {"genotypes-without-rowid", no_argument, 0, 'R'},
                                           {"table-prefix", required_argument, 0, 't'},
                                           {"no-gri", no_argument, 0, 'G'},
                                           {0, 0, 0, 0}};

    int c;
    while (-1 != (c = getopt_long(argc, argv, "hqGRl:t:p:", long_options, nullptr))) {
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
        case 'R':
            genotypes_without_rowid = true;
            break;
        case 't':
            table_prefix = optarg;
            break;
        case 'p':
            errno = 0;
            ploidy = strtol(optarg, nullptr, 10);
            if (errno || ploidy < 1) {
                cerr << "vcf_into_sqlite: couldn't parse --ploidy > 0";
                return -1;
            }
            break;
        case 'I':
            errno = 0;
            inner_page_KiB = strtol(optarg, nullptr, 10);
            if (errno || inner_page_KiB < 1 || inner_page_KiB > 64) {
                cerr << "vcf_into_sqlite: invalid --inner-page-KiB" << endl;
                return -1;
            }
            break;
        case 'O':
            errno = 0;
            outer_page_KiB = strtol(optarg, nullptr, 10);
            if (errno || outer_page_KiB < 1 || outer_page_KiB > 64) {
                cerr << "vcf_into_sqlite: invalid --outer-page-KiB" << endl;
                return -1;
            }
            break;
        case 'l':
            errno = 0;
            level = strtol(optarg, nullptr, 10);
            if (errno || level < -7 || level > 22) {
                cerr << "vcf_into_sqlite: couldn't parse --level in [-7,22]";
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

    // open infilename & read VCF header
    unique_ptr<vcfFile, void (*)(vcfFile *)> vcf(bcf_open(infilename.c_str(), "r"),
                                                 [](vcfFile *f) { bcf_close(f); });
    if (!vcf) {
        cerr << "vcf_into_sqlite: failed opening " << infilename << endl;
        return 1;
    }
    hts_set_threads(vcf.get(), max(2U, thread::hardware_concurrency() / 4));
    unique_ptr<bcf_hdr_t, void (*)(bcf_hdr_t *)> hdr(bcf_hdr_read(vcf.get()), &bcf_hdr_destroy);
    if (!hdr) {
        cerr << "vcf_into_sqlite: failed reading VCF header from " << infilename << '\n';
        return 1;
    }

    try {
        auto info_hrecs = extract_hrecs(hdr.get(), "INFO", {"ID", "Number", "Type"});
        auto format_hrecs = extract_hrecs(hdr.get(), "FORMAT", {"ID", "Number", "Type"});
        if (!bcf_hdr_nsamples(hdr.get())) {
            format_hrecs.clear();
        }

        // open output database
        GENOMICSQLITE_CXX_INIT();
        sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);
        sqlite3_config(SQLITE_CONFIG_LOOKASIDE, 2048, 128);
        string config_json = R"({"unsafe_load": true, "zstd_level":)" + to_string(level) +
                             R"(,"inner_page_KiB":)" + to_string(inner_page_KiB) +
                             R"(,"outer_page_KiB":)" + to_string(outer_page_KiB) + "}";
        auto db = GenomicSQLiteOpen(
            outfilename, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX,
            config_json);
#ifndef NDEBUG
        db->exec("PRAGMA foreign_keys=ON");
#endif
        SQLite::Transaction txn(*db);

        // import contigs
        // TODO: allow to open existing db with existing, consistent assembly
        int rid = 0;
        string assembly;
        for (auto &ctg : extract_hrecs(hdr.get(), "contig", {"ID", "length"})) {
            auto p = ctg.find("assembly");
            if (!rid && assembly.empty()) {
                if (p != ctg.end()) {
                    assembly = p->second;
                }
            }
            if (p != ctg.end() && p->second != assembly) {
                throw runtime_error(
                    "unexpected: VCF header contig lines reference multiple assemblies");
            }
            errno = 0;
            sqlite3_int64 length = strtoull(ctg["length"].c_str(), nullptr, 10);
            if (length <= 0 || errno) {
                throw runtime_error("invalid contig length in VCF header");
            }
            string sql = PutGenomicReferenceSequenceSQL(ctg["ID"], length, assembly, "", "{}", rid);
            if (rid == 0) {
                progress &&cerr << sql << endl;
            } else if (rid == 1) {
                progress &&cerr << "  ..." << endl;
            }
            db->exec(sql);
            rid++;
        }
        if (!rid) {
            throw runtime_error("VCF header must specify contigs");
        }

        // formulate & apply DDL
        // TODO: allow --append
        string ddl =
            schemaDDL(table_prefix, info_hrecs, format_hrecs, ploidy, genotypes_without_rowid);
        progress &&cerr << ddl << endl;
        db->exec(ddl);

        if (!format_hrecs.empty()) {
            progress &&cerr << "inserting " << bcf_hdr_nsamples(hdr.get()) << " samples..." << endl;
            insert_samples(hdr.get(), table_prefix, *db);
        }

        // process VCF records
        auto insert_variant_stmt = prepare_insert_variant(table_prefix, info_hrecs, *db);
        unique_ptr<SQLite::Statement> insert_genotype_stmt;
        progress &&cerr << "inserting variants"
                        << (format_hrecs.empty() ? "..." : " & genotypes...") << endl;

        BCFReader reader(vcf.get(), hdr.get(), 64);
        while (reader.next()) {
            bcf1_t *rec = reader.item().get();
            assert(rec);
            try {
                insert_variant(hdr.get(), rec, info_hrecs, *insert_variant_stmt);
                if (!format_hrecs.empty()) {
                    if (!insert_genotype_stmt) {
                        insert_genotype_stmt =
                            prepare_insert_genotype(table_prefix, format_hrecs, ploidy, *db);
                    }
                    insert_genotypes(hdr.get(), rec, format_hrecs, ploidy, db->getLastInsertRowid(),
                                     *insert_genotype_stmt);
                }
            } catch (exception &exn) {
                reader.abort();
                throw exn;
            }
        }
        progress &&cerr << reader.log() << endl;

        // create GRI
        if (gri) {
            progress &&cerr << "genomic range indexing..." << endl;
            string gri_sql =
                CreateGenomicRangeIndexSQL(table_prefix + "variants", "rid", "pos", "pos+rlen");
            progress &&cerr << gri_sql << endl;
            db->exec(gri_sql);
        }

        progress &&cerr << "COMMIT" << endl;
        txn.commit();
    } catch (exception &exn) {
        cerr << "vcf_into_sqlite: " << exn.what() << '\n';
        return 1;
    }

    return 0;
}
