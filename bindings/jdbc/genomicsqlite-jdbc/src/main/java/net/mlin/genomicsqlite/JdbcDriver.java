/* based on JDBC.java in sqlite-jdbc */
/*
 * Copyright (c) 2007 David Crawshaw <david@zentus.com>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package net.mlin.genomicsqlite;

import java.io.*;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import org.sqlite.SQLiteConnection;

public class JdbcDriver extends org.sqlite.JDBC {
  public static final String PREFIX = "jdbc:genomicsqlite:";
  private static Connection memoryDb = null;
  // how getLibraryFilename() obtained the library, for load-failure messages
  private static String librarySource = "the system library search path";

  static {
    try {
      DriverManager.registerDriver(new JdbcDriver());
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * @see java.sql.Driver#acceptsURL(java.lang.String)
   */
  @Override
  public boolean acceptsURL(String url) {
    return isValidURL(url);
  }

  /**
   * Validates a URL
   *
   * @param url
   * @return true if the URL is valid, false otherwise
   */
  public static boolean isValidURL(String url) {
    return url != null && url.toLowerCase().startsWith(PREFIX);
  }

  /**
   * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
   */
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return createConnection(url, info);
  }

  /**
   * Gets the location to the database from a given URL.
   *
   * @param url The URL to extract the location from.
   * @return The location to the database.
   */
  static String extractAddress(String url) {
    return url.substring(PREFIX.length());
  }

  /**
   * Creates a new database connection to a given URL.
   *
   * @param url the URL
   * @param prop the properties
   * @return a Connection object that represents a connection to the URL
   * @throws SQLException
   * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
   */
  public static SQLiteConnection createConnection(String url, Properties prop) throws SQLException {
    if (!isValidURL(url)) {
      return null;
    }
    url = url.trim();
    String dbfn = extractAddress(url);
    // Retrieve GenomicSQLite configuration settings as a JSON string for now. It'd be nice to
    // admit individual settings through Properties; we just need to namespace them to avoid any
    // future conflict with settings to be passed through to sqlite-jdbc.
    prop = prop != null ? prop : new java.util.Properties();
    String config_json = prop.getProperty("genomicsqlite.config_json", "{}");

    ensureExtension();
    PreparedStatement stmt;
    ResultSet row;
    synchronized (memoryDb) {
      stmt = memoryDb.prepareStatement("SELECT genomicsqlite_uri(?,?)");
      stmt.setString(1, dbfn);
      stmt.setString(2, config_json);
      row = stmt.executeQuery();
      row.next();
      url = row.getString(1);
    }

    if (prop.getProperty("open_mode") == null
        && (url.indexOf("&mode=ro") > -1 || url.indexOf("&immutable=1") > -1)) {
      prop.setProperty("open_mode", "65"); // SQLITE_OPEN_READONLY|SQLITE_OPEN_URI
    }
    SQLiteConnection conn = org.sqlite.JDBC.createConnection("jdbc:sqlite:" + url, prop);

    stmt = conn.prepareStatement("SELECT genomicsqlite_tuning_sql(?)");
    stmt.setString(1, config_json);
    row = stmt.executeQuery();
    row.next();
    conn.createStatement().executeUpdate(row.getString(1));
    return conn;
  }

  static synchronized void ensureExtension() throws SQLException {
    if (memoryDb == null) {
      String libraryFilename = getLibraryFilename();

      // FIXME: when we wrote this, sqlite-jdbc did not have a wrapper for sqlite3_load_extension()
      // so we go through SQL which is a small security concern. Pending:
      //   https://github.com/xerial/sqlite-jdbc/pull/319
      Properties memProp = new Properties();
      memProp.setProperty("enable_load_extension", "true");
      Connection db = DriverManager.getConnection("jdbc:sqlite::memory:", memProp);
      String loadName = libraryFilename != null ? libraryFilename : "libgenomicsqlite";
      PreparedStatement stmt = db.prepareStatement("SELECT load_extension(?)");
      stmt.setString(1, loadName);
      try {
        stmt.execute();
      } catch (SQLException exc) {
        throw new SQLException(
            "genomicsqlite: could not load "
                + loadName
                + " (from "
                + librarySource
                + "): "
                + exc.getMessage()
                + "; set LIBGENOMICSQLITE to the library file, or install libgenomicsqlite"
                + " where the system loader finds it",
            exc);
      }
      memoryDb = db;
    }
  }

  static String getLibraryFilename() {
    // Return an override or extracted bundle path; null requests system-loader search.
    librarySource = "the system library search path";
    String override = trimmedOverride(System.getenv("LIBGENOMICSQLITE"));
    if (override != null) {
      librarySource = "the LIBGENOMICSQLITE environment variable";
      return override;
    }
    String ans = null;
    String resourcePath = getLibraryResourcePath();
    if (resourcePath != null) {
      try {
        InputStream reader = JdbcDriver.class.getClassLoader().getResourceAsStream(resourcePath);
        if (reader != null) {
          String tempDirpath =
              Files.createTempDirectory("genomicsqlite-jdbc-").toAbsolutePath().toString();
          (new File(tempDirpath)).deleteOnExit();
          // Extract under the plain filename; target variation belongs in resource paths.
          File extracted =
              new File(tempDirpath, resourcePath.substring(resourcePath.lastIndexOf('/') + 1));
          FileOutputStream writer = new FileOutputStream(extracted.getAbsolutePath());
          extracted.deleteOnExit();
          byte[] buffer = new byte[65536];
          int bytesRead = 0;
          while ((bytesRead = reader.read(buffer)) != -1) {
            writer.write(buffer, 0, bytesRead);
          }
          extracted.setReadable(true);
          extracted.setExecutable(true);
          ans = extracted.getAbsolutePath();
          librarySource = "the copy bundled in this JAR";
        }
      } catch (IOException exc) {
      }
    }
    return ans;
  }

  static String trimmedOverride(String override) {
    // String.trim() does not cover Unicode whitespace; whitespace-only means unset.
    if (override == null) {
      return null;
    }
    String trimmed = override.replaceAll("(?U)^\\s+|\\s+$", "");
    return trimmed.length() > 0 ? trimmed : null;
  }

  static String getLibraryResourcePath() {
    // Return the canonical resource path, or null for an unrecognized OS or CPU.
    // Missing resources fall back to system search.
    String arch = System.getProperty("os.arch", "unknown").toLowerCase(Locale.ENGLISH);
    String archTok = null;
    if (arch.equals("amd64") || arch.equals("x86_64")) {
      archTok = "x86_64";
    } else if (arch.equals("aarch64") || arch.equals("arm64")) {
      archTok = "aarch64";
    }
    if (archTok == null) {
      return null;
    }
    String os = System.getProperty("os.name", "unknown").toLowerCase(Locale.ENGLISH);
    if (os.indexOf("mac") >= 0 || os.indexOf("darwin") >= 0) {
      return archTok + "-darwin/libgenomicsqlite.dylib";
    } else if (os.indexOf("nux") >= 0) {
      return archTok + "-linux-gnu/libgenomicsqlite.so";
    }
    return null;
  }
}
