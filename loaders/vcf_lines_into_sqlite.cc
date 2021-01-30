/*
 * vcf_lines_into_sqlite: load VCF into a simple GenomicSQLite database which merely stores each
 * text line alongside bare-essential columns for genomic range indexing (CHROM,POS,rlen). The
 * header is jammed into a row with null positions.
 */
#include "common.hpp"
#include <fstream>
#include <getopt.h>
#include <unistd.h>
#include <vector>

void InsertLine(const string &line, SQLite::Statement &stmt) {
    stmt.reset();

    string line2(line);
    vector<char *> fields;
    split(line2, '\t', back_inserter(fields), 9);
    if (fields.size() < 8) {
        throw runtime_error("invalid VCF: " + line);
    }

    // CHROM
    stmt.bind(1, fields[0]);

    // POS
    errno = 0;
    sqlite3_int64 pos = strtoll(fields[1], nullptr, 10);
    if (errno || pos < 1) {
        throw runtime_error("invalid POS " + string(fields[1]));
    }
    --pos; // make zero-based
    stmt.bind(2, pos);

    // rlen
    if (strncmp(fields[7], "END=", 4)) {
        stmt.bind(3, sqlite3_int64(strlen(fields[3])));
    } else {
        vector<char *> info;
        split(fields[7], ';', back_inserter(info), 2);
        sqlite3_int64 end = strtoll(info[0] + 4, nullptr, 10);
        if (errno || end < pos) {
            throw runtime_error("invalid END " + string(fields[4]));
        }
        stmt.bind(3, end - pos);
    }

    stmt.bindNoCopy(4, line);
    stmt.exec();
}

void help() {
    cout
        << "vcf_lines_into_sqlite: import .vcf, .vcf.gz, or .bcf lines into simple GenomicSQLite table"
        << '\n'
        << GIT_REVISION << "   " << __DATE__ << '\n'
        << '\n'
        << "bgzip -dc in.vcf.gz | vcf_lines_into_sqlite [options] out.db" << '\n'
        << "Options: " << '\n'
        << "  --table NAME           table name (default: vcf_lines)" << '\n'
        << "  --no-gri               skip genomic range indexing" << '\n'
        << "  -l,--level LEVEL       database compression level (-7 to 22, default 6)" << '\n'
        << "  -q,--quiet             suppress progress information on standard error" << '\n'
        << "  -h,--help              show this help message" << '\n'
        << '\n';
}

int main(int argc, char *argv[]) {
    string table("vcf_lines"), outfilename;
    bool gri = true, progress = true;
    int level = 6;

    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},        {"quiet", no_argument, 0, 'q'},
        {"level", required_argument, 0, 'l'}, {"table", required_argument, 0, 't'},
        {"no-gri", no_argument, 0, 'G'},      {0, 0, 0, 0}};

    int c;
    while (-1 != (c = getopt_long(argc, argv, "hqGl:t:", long_options, nullptr))) {
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
        case 't':
            table = optarg;
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

    if (optind != argc - 1) {
        help();
        return -1;
    }

    outfilename = argv[argc - 1];
    if (isatty(STDIN_FILENO)) {
        help();
        return -1;
    }
    ios_base::sync_with_stdio(false);
    cin.tie(nullptr);

    try {
        OStringStream hdr;
        string input_line;
        while (getline(cin, input_line)) {
            if (input_line.empty()) {
                throw runtime_error("vcf_lines_into_sqlite: unexpected empty line");
                return 1;
            }
            if (input_line[0] == '#') {
                hdr << input_line << '\n';
            } else {
                break;
            }
        }
        if (cin.bad()) {
            throw runtime_error("vcf_lines_into_sqlite: input read error");
            return 1;
        }

        // open output database
        GENOMICSQLITE_CXX_INIT();
        sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);
        sqlite3_config(SQLITE_CONFIG_LOOKASIDE, 2048, 128);
        auto db = GenomicSQLiteOpen(
            outfilename, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX,
            R"( {"unsafe_load": true, "zstd_level": )" + to_string(level) + "}");
        SQLite::Transaction txn(*db);

        db->exec("CREATE TABLE " + table + "(CHROM TEXT, POS INTEGER, rlen INTEGER, line TEXT)");

        // prepare insertion statement
        SQLite::Statement stmt_insert(*db, "INSERT INTO " + table +
                                               "(CHROM,POS,rlen,line) VALUES(?,?,?,?)");

        // insert header
        stmt_insert.bindNoCopy(4, hdr.Get());
        stmt_insert.exec();

        int count = 0;
        do {
            InsertLine(input_line, stmt_insert);
            ++count;
        } while (getline(cin, input_line));
        if (!cin.eof() || cin.bad()) {
            throw runtime_error("vcf_lines_into_sqlite: input read error");
            return 1;
        }
        progress &&cerr << "inserted " << count << " lines" << endl;

        // create GRI
        if (gri) {
            progress &&cerr << "genomic range indexing..." << endl;
            string gri_sql = CreateGenomicRangeIndexSQL(table, "CHROM", "POS", "POS+rlen");
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
