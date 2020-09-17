package net.mlin.genomicsqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class GenomicSQLite {
  public static String version(Connection conn) throws SQLException {
    ResultSet row = conn.createStatement().executeQuery("SELECT genomicsqlite_version()");
    row.next();
    return row.getString(1);
  }

  public static String attachSQL(
      Connection conn, String dbFileName, String schemaName, String configJson)
      throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("SELECT genomicsqlite_attach_sql(?,?,?)");
    stmt.setString(1, dbFileName);
    stmt.setString(2, schemaName);
    stmt.setString(3, configJson);
    ResultSet row = stmt.executeQuery();
    row.next();
    return row.getString(1);
  }

  public static String attachSQL(Connection conn, String dbFileName, String schemaName)
      throws SQLException {
    return attachSQL(conn, dbFileName, schemaName, "{}");
  }

  public static String vacuumIntoSQL(Connection conn, String destFileName, String configJson)
      throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("SELECT genomicsqlite_vacuum_into_sql(?,?)");
    stmt.setString(1, destFileName);
    stmt.setString(2, configJson);
    ResultSet row = stmt.executeQuery();
    row.next();
    return row.getString(1);
  }

  public static String vacuumIntoSQL(Connection conn, String destFileName) throws SQLException {
    return vacuumIntoSQL(conn, destFileName, "{}");
  }

  public static String createGenomicRangeIndexSQL(
      Connection conn,
      String tableName,
      String rid,
      String beginPosition,
      String endPosition,
      int floor)
      throws SQLException {
    PreparedStatement stmt =
        conn.prepareStatement("SELECT create_genomic_range_index_sql(?,?,?,?,?)");
    stmt.setString(1, tableName);
    stmt.setString(2, rid);
    stmt.setString(3, beginPosition);
    stmt.setString(4, endPosition);
    stmt.setInt(5, floor);
    ResultSet row = stmt.executeQuery();
    row.next();
    return row.getString(1);
  }

  public static String createGenomicRangeIndexSQL(
      Connection conn, String tableName, String rid, String beginPosition, String endPosition)
      throws SQLException {
    return createGenomicRangeIndexSQL(conn, tableName, rid, beginPosition, endPosition, -1);
  }

  public static String putReferenceAssemblySQL(Connection conn, String assembly)
      throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("SELECT put_genomic_reference_assembly_sql(?)");
    stmt.setString(1, assembly);
    ResultSet row = stmt.executeQuery();
    row.next();
    return row.getString(1);
  }

  public static String putReferenceSequenceSQL(
      Connection conn,
      String name,
      long length,
      String assembly,
      String refget_id,
      String meta_json,
      long rid)
      throws SQLException {
    PreparedStatement stmt =
        conn.prepareStatement("SELECT put_genomic_reference_sequence_sql(?,?,?,?,?,?)");
    stmt.setString(1, name);
    stmt.setLong(2, length);
    stmt.setString(3, assembly);
    stmt.setString(4, refget_id);
    stmt.setString(5, meta_json);
    stmt.setLong(6, rid);
    ResultSet row = stmt.executeQuery();
    row.next();
    return row.getString(1);
  }

  public static String putReferenceSequenceSQL(
      Connection conn,
      String name,
      long length,
      String assembly,
      String refget_id,
      String meta_json)
      throws SQLException {
    PreparedStatement stmt =
        conn.prepareStatement("SELECT put_genomic_reference_sequence_sql(?,?,?,?,?)");
    stmt.setString(1, name);
    stmt.setLong(2, length);
    if (assembly != null) stmt.setString(3, assembly);
    if (refget_id != null) stmt.setString(4, refget_id);
    if (meta_json != null) stmt.setString(5, meta_json);
    ResultSet row = stmt.executeQuery();
    row.next();
    return row.getString(1);
  }

  public static String putReferenceSequenceSQL(
      Connection conn, String name, long length, String assembly, String refget_id)
      throws SQLException {
    return putReferenceSequenceSQL(conn, name, length, assembly, refget_id, null);
  }

  public static String putReferenceSequenceSQL(
      Connection conn, String name, long length, String assembly) throws SQLException {
    return putReferenceSequenceSQL(conn, name, length, assembly, null, null);
  }

  public static String putReferenceSequenceSQL(Connection conn, String name, long length)
      throws SQLException {
    return putReferenceSequenceSQL(conn, name, length, null, null, null);
  }

  public static HashMap<Long, ReferenceSequence> getReferenceSequencesByRid(
      Connection conn, String assembly) throws SQLException {
    String sql =
        "SELECT _gri_rid, gri_refseq_name, gri_refseq_length, gri_assembly, gri_refget_id, gri_refseq_meta_json FROM _gri_refseq";
    PreparedStatement stmt;
    if (assembly != null) {
      stmt = conn.prepareStatement(sql + " WHERE gri_assembly = ?");
      stmt.setString(1, assembly);
    } else {
      stmt = conn.prepareStatement(sql);
    }
    ResultSet row = stmt.executeQuery();
    HashMap<Long, ReferenceSequence> ans = new HashMap<Long, ReferenceSequence>();
    while (row.next()) {
      ReferenceSequence grs =
          new ReferenceSequence(
              row.getLong(1),
              row.getString(2),
              row.getLong(3),
              row.getString(4),
              row.getString(5),
              row.getString(6));
      ans.put(grs.rid, grs);
    }
    return ans;
  }

  public static HashMap<Long, ReferenceSequence> getReferenceSequencesByRid(Connection conn)
      throws SQLException {
    return getReferenceSequencesByRid(conn, null);
  }

  public static HashMap<String, ReferenceSequence> getReferenceSequencesByName(
      Connection conn, String assembly) throws SQLException {
    HashMap<String, ReferenceSequence> ans = new HashMap<String, ReferenceSequence>();
    for (HashMap.Entry<Long, ReferenceSequence> entry :
        getReferenceSequencesByRid(conn, assembly).entrySet()) {
      ReferenceSequence grs = entry.getValue();
      if (ans.containsKey(grs.name)) {
        throw new RuntimeException(
            "GenomicSQLite.getReferenceSequencesByName: duplicate name; try filtering by assembly");
      }
      ans.put(grs.name, grs);
    }
    return ans;
  }

  public static HashMap<String, ReferenceSequence> getReferenceSequencesByName(Connection conn)
      throws SQLException {
    return getReferenceSequencesByName(conn, null);
  }
}
