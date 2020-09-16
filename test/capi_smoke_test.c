#include "genomicsqlite.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int sql_callback(void *ctx, int nCol, char **values, char **names) {
    fprintf(stderr, "sql_callback(%d,\"%s\")\n", nCol, values[0] ? values[0] : "");
    char *ans = (char *)ctx;
    if (!ans || !nCol || !values[0]) {
        return -1;
    }
    strcpy(ans, values[0]);
    return 0;
}

int main(int argc, char **argv) {
    fprintf(stderr, "tempnam()\n");
    char *dbfilename = tempnam("/tmp", "gsqlt");
    if (!dbfilename) {
        fprintf(stderr, "tempnam -> %d\n", errno);
        return 1;
    }
    sqlite3 *pDb = 0;
    char *zErrMsg = 0;
    fprintf(stderr, "genomicsqlite_open()\n");
    int rc = genomicsqlite_open(dbfilename, &pDb, &zErrMsg,
                                SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, "{}");
    if (rc != SQLITE_OK) {
        fprintf(stderr, "genomicsqlite_open -> %d // %s\n", rc,
                zErrMsg ? zErrMsg : sqlite3_errstr(rc));
        return 1;
    }

    fprintf(stderr, "genomicsqlite_version()\n");
    char *version1 = genomicsqlite_version();
    printf("%s\n", version1);
    fprintf(stderr, "sqlite3_free()\n");
    sqlite3_free(version1);

    char version[1024];
    version[0] = 0;
    fprintf(stderr, "sqlite3_exec()\n");
    rc = sqlite3_exec(pDb, "SELECT genomicsqlite_version()", &sql_callback, version, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec(\"SELECT genomicsqlite_version()\") -> %d // %s\n", rc,
                zErrMsg ? zErrMsg : sqlite3_errstr(rc));
        return 1;
    }
    if (!version[0]) {
        fprintf(stderr, "no result from sqlite3_exec(\"SELECT genomicsqlite_version()\")\n");
        return 1;
    }
    printf("%s\n", version);

    fprintf(stderr, "put_genomic_reference_assembly_sql() @%d\n", sqlite3_total_changes(pDb));
    char *sql = put_genomic_reference_assembly_sql("GRCh38_no_alt_analysis_set", 0);
    if (!sql) {
        fprintf(stderr, "put_genomic_reference_assembly_sql -> null\n");
        return 1;
    }
    rc = sqlite3_exec(pDb, sql, 0, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr,
                "sqlite3_exec(\"put_genomic_reference_assembly_sql(GRCh38))\") -> %d // %s\n", rc,
                zErrMsg ? zErrMsg : sqlite3_errstr(rc));
        return 1;
    }

    fprintf(stderr, "create_genomic_range_index_sql() @%d\n", sqlite3_total_changes(pDb));
    sql = create_genomic_range_index_sql("test", "rid", "beg", "end", -1);
    if (!sql) {
        fprintf(stderr, "create_genomic_range_index_sql -> null\n");
        return 1;
    }
    printf("%s\n", sql);
    fprintf(stderr, "sqlite3_free()\n");
    sqlite3_free(sql);

    fprintf(stderr, "sqlite3_close()\n");
    rc = sqlite3_close(pDb);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close -> %d %s\n", rc, sqlite3_errstr(rc));
        return 1;
    }
    return 0;
}
