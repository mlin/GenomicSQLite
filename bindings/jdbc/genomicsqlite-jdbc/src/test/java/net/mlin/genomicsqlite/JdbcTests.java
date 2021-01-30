package net.mlin.genomicsqlite;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import org.junit.Test;

public class JdbcTests {
  @Test
  public void smokeTest() throws SQLException {
    // create temp database
    String url =
        "jdbc:genomicsqlite:/tmp/GenomicSQLiteJDBC." + java.util.UUID.randomUUID().toString();
    assertTrue(JdbcDriver.isValidURL(url)); // triggers driver registration absent JAR
    Properties prop = new Properties();
    prop.setProperty("genomicsqlite.config_json", "{\"threads\": 3}");
    Connection conn = DriverManager.getConnection(url, prop);
    System.err.println(GenomicSQLite.version(conn));

    // check that tuning SQL applied successfully
    ResultSet row = conn.createStatement().executeQuery("PRAGMA threads");
    row.next();
    assertEquals(3, row.getInt(1));

    // load table & create GRI
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("CREATE TABLE feature(rid INTEGER, beg INTEGER, end INTEGER)");
    stmt.executeUpdate("INSERT INTO feature VALUES(3, 12, 34)");
    stmt.executeUpdate("INSERT INTO feature VALUES(3, 0, 23)");
    stmt.executeUpdate("INSERT INTO feature VALUES(3, 34, 56)");
    stmt.executeUpdate(
        GenomicSQLite.createGenomicRangeIndexSQL(conn, "feature", "rid", "beg", "end"));

    // GRI query
    row = stmt.executeQuery("SELECT COUNT(*) FROM genomic_range_rowids('feature',3,34,34)");
    row.next();
    assertEquals(2, row.getInt(1));

    // _gri_refseq stuff
    stmt.executeUpdate(GenomicSQLite.putReferenceAssemblySQL(conn, "GRCh38_no_alt_analysis_set"));
    HashMap<String, ReferenceSequence> refs = GenomicSQLite.getReferenceSequencesByName(conn);
    ReferenceSequence chr3 = refs.get("chr3");
    assertEquals("chr3", chr3.name);
    assertEquals(3, chr3.rid);
    assertEquals(198295559, chr3.length);
    assertEquals("{}", chr3.metaJson);
  }

  @Test
  public void webTest() throws SQLException {
    String url =
        "https://github.com/mlin/sqlite_zstd_vfs/releases/download/web-test-db-v1/TxDb.Hsapiens.UCSC.hg38.knownGene.vacuum.genomicsqlite";
    url = "jdbc:genomicsqlite:" + url;
    assertTrue(JdbcDriver.isValidURL(url)); // triggers driver registration absent JAR
    Properties prop = new Properties();
    prop.setProperty("genomicsqlite.config_json", "{\"threads\": 3}");
    Connection conn = DriverManager.getConnection(url, prop);
    ResultSet row = conn.createStatement().executeQuery("SELECT COUNT(*) FROM sqlite_master");
    row.next();
    assertEquals(12, row.getInt(1));
    conn.close();
  }
}
