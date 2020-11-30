package net.mlin.genomicsqlite;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import net.mlin.genomicsqlite.GenomicSQLite;

/**
 * Hello world!
 *
 */
public class MavenSmokeTest 
{
    public static void main( String[] args ) throws SQLException
    {
        String url =
            "jdbc:genomicsqlite:/tmp/GenomicSQLiteJDBC." + java.util.UUID.randomUUID().toString();
        Connection conn = DriverManager.getConnection(url);
        System.out.println("GenomicSQLite " + GenomicSQLite.version(conn));
    }
}
