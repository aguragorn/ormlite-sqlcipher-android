package com.j256.ormlite.sqlcipher.android.apptools

import android.content.Context
import com.j256.ormlite.dao.Dao
import com.j256.ormlite.dao.DaoManager
import com.j256.ormlite.dao.RuntimeExceptionDao
import com.j256.ormlite.logger.Logger
import com.j256.ormlite.logger.LoggerFactory
import com.j256.ormlite.sqlcipher.android.AndroidConnectionSource
import com.j256.ormlite.sqlcipher.android.AndroidDatabaseConnection
import com.j256.ormlite.support.ConnectionSource
import com.j256.ormlite.support.DatabaseConnection
import com.j256.ormlite.table.DatabaseTableConfigLoader
import net.sqlcipher.database.SQLiteDatabase
import net.sqlcipher.database.SQLiteDatabase.CursorFactory
import net.sqlcipher.database.SQLiteDatabaseHook
import net.sqlcipher.database.SQLiteOpenHelper

import java.io.*
import java.sql.SQLException

/**
 * SQLite database open helper which can be extended by your application to help manage when the application needs to
 * create or upgrade its database.
 *
 * @author kevingalligan, graywatson
 */
abstract class OrmLiteSqliteOpenHelper : SQLiteOpenHelper {
    protected var connectionSource = AndroidConnectionSource(this)

    protected var cancelQueriesEnabled: Boolean = false
    /**
     * Return true if the helper is still open. Once [.close] is called then this will return false.
     */
    @Volatile
    var isOpen = true
        private set
    var password: String? = null

    /**
     * @param context
     * Associated content from the application. This is needed to locate the database.
     * @param databaseName
     * Name of the database we are opening.
     * @param factory
     * Cursor factory or null if none.
     * @param databaseVersion
     * Version of the database we are opening. This causes [.onUpgrade] to be
     * called if the stored database is a different version.
     */
    constructor(
        context: Context,
        databaseName: String,
        factory: CursorFactory,
        databaseVersion: Int
    ) : super(context, databaseName, factory, databaseVersion) {
        logger.trace("{}: constructed connectionSource {}", this, connectionSource)
    }

    /**
     * Same as the other constructor with the addition of a file-id of the table config-file. See
     * [OrmLiteConfigUtil] for details.
     *
     * @param context
     * Associated content from the application. This is needed to locate the database.
     * @param databaseName
     * Name of the database we are opening.
     * @param factory
     * Cursor factory or null if none.
     * @param databaseVersion
     * Version of the database we are opening. This causes [.onUpgrade] to be
     * called if the stored database is a different version.
     * @param configFileId
     * file-id which probably should be a R.raw.ormlite_config.txt or some static value.
     */
    constructor(
        context: Context, databaseName: String, factory: CursorFactory, databaseVersion: Int,
        configFileId: Int, password: String
    ) : this(
        context,
        databaseName,
        factory,
        databaseVersion,
        openFileId(context, configFileId),
        password
    ) {
    }

    /**
     * Same as the other constructor with the addition of a config-file. See [OrmLiteConfigUtil] for details.
     *
     * @param context
     * Associated content from the application. This is needed to locate the database.
     * @param databaseName
     * Name of the database we are opening.
     * @param factory
     * Cursor factory or null if none.
     * @param databaseVersion
     * Version of the database we are opening. This causes [.onUpgrade] to be
     * called if the stored database is a different version.
     * @param configFile
     * Configuration file to be loaded.
     */
    constructor(
        context: Context, databaseName: String, factory: CursorFactory, databaseVersion: Int,
        configFile: File, password: String
    ) : this(context, databaseName, factory, databaseVersion, openFile(configFile), password) {
    }

    /**
     * Same as the other constructor with the addition of a input stream to the table config-file. See
     * [OrmLiteConfigUtil] for details.
     *
     * @param context
     * Associated content from the application. This is needed to locate the database.
     * @param databaseName
     * Name of the database we are opening.
     * @param factory
     * Cursor factory or null if none.
     * @param databaseVersion
     * Version of the database we are opening. This causes [.onUpgrade] to be
     * called if the stored database is a different version.
     * @param stream
     * Stream opened to the configuration file to be loaded.
     */
    constructor(
        context: Context, databaseName: String, factory: CursorFactory, databaseVersion: Int,
        stream: InputStream?, password: String
    ) : super(context, databaseName, factory, databaseVersion) {
        this.password = password
        if (stream == null) {
            return
        }

        // if a config file-id was specified then load it into the DaoManager
        try {
            val reader = BufferedReader(InputStreamReader(stream), 4096)
            DaoManager.addCachedDatabaseConfigs(
                DatabaseTableConfigLoader.loadDatabaseConfigFromReader(
                    reader
                )
            )
        } catch (e: SQLException) {
            throw IllegalStateException("Could not load object config file", e)
        } finally {
            try {
                // we close the stream here because we may not get a reader
                stream.close()
            } catch (e: IOException) {
                // ignore close errors
            }

        }
    }

    constructor(
        context: Context,
        databaseName: String,
        databaseVersion: Int,
        password: String,
        databaseHook: SQLiteDatabaseHook
    ): super(context, databaseName, null, databaseVersion, databaseHook) {
        this.password = password
    }

    /**
     * What to do when your database needs to be created. Usually this entails creating the tables and loading any
     * initial data.
     *
     *
     *
     * **NOTE:** You should use the connectionSource argument that is passed into this method call or the one
     * returned by getConnectionSource(). If you use your own, a recursive call or other unexpected results may result.
     *
     *
     * @param database
     * Database being created.
     * @param connectionSource
     * To use get connections to the database to be created.
     */
    abstract fun onCreate(database: SQLiteDatabase, connectionSource: ConnectionSource)

    /**
     * What to do when your database needs to be updated. This could mean careful migration of old data to new data.
     * Maybe adding or deleting database columns, etc..
     *
     *
     *
     * **NOTE:** You should use the connectionSource argument that is passed into this method call or the one
     * returned by getConnectionSource(). If you use your own, a recursive call or other unexpected results may result.
     *
     *
     * @param database
     * Database being upgraded.
     * @param connectionSource
     * To use get connections to the database to be updated.
     * @param oldVersion
     * The version of the current database so we can know what to do to the database.
     * @param newVersion
     * The version that we are upgrading the database to.
     */
    abstract fun onUpgrade(
        database: SQLiteDatabase, connectionSource: ConnectionSource, oldVersion: Int,
        newVersion: Int
    )

    /**
     * Get the connection source associated with the helper.
     */
    fun getConnectionSource(): ConnectionSource {
        if (!isOpen) {
            // we don't throw this exception, but log it for debugging purposes
            logger.warn(IllegalStateException(), "Getting connectionSource was called after closed")
        }
        return connectionSource
    }

    /**
     * Satisfies the [SQLiteOpenHelper.onCreate] interface method.
     */
    override fun onCreate(db: SQLiteDatabase) {
        val cs = getConnectionSource()
        /*
		 * The method is called by Android database helper's get-database calls when Android detects that we need to
		 * create or update the database. So we have to use the database argument and save a connection to it on the
		 * AndroidConnectionSource, otherwise it will go recursive if the subclass calls getConnectionSource().
		 */
        var conn: DatabaseConnection? = cs.specialConnection
        var clearSpecial = false
        if (conn == null) {
            conn = AndroidDatabaseConnection(db, true, cancelQueriesEnabled)
            try {
                cs.saveSpecialConnection(conn)
                clearSpecial = true
            } catch (e: SQLException) {
                throw IllegalStateException("Could not save special connection", e)
            }

        }
        try {
            onCreate(db, cs)
        } finally {
            if (clearSpecial) {
                cs.clearSpecialConnection(conn)
            }
        }
    }

    /**
     * Satisfies the [SQLiteOpenHelper.onUpgrade] interface method.
     */
    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
        val cs = getConnectionSource()
        /*
		 * The method is called by Android database helper's get-database calls when Android detects that we need to
		 * create or update the database. So we have to use the database argument and save a connection to it on the
		 * AndroidConnectionSource, otherwise it will go recursive if the subclass calls getConnectionSource().
		 */
        var conn: DatabaseConnection? = cs.specialConnection
        var clearSpecial = false
        if (conn == null) {
            conn = AndroidDatabaseConnection(db, true, cancelQueriesEnabled)
            try {
                cs.saveSpecialConnection(conn)
                clearSpecial = true
            } catch (e: SQLException) {
                throw IllegalStateException("Could not save special connection", e)
            }

        }
        try {
            onUpgrade(db, cs, oldVersion, newVersion)
        } finally {
            if (clearSpecial) {
                cs.clearSpecialConnection(conn)
            }
        }
    }

    /**
     * Close any open connections.
     */
    override fun close() {
        super.close()
        connectionSource.close()
        /*
		 * We used to set connectionSource to null here but now we just set the closed flag and then log heavily if
		 * someone uses getConectionSource() after this point.
		 */
        isOpen = false
    }

    /**
     * Get a DAO for our class. This uses the [DaoManager] to cache the DAO for future gets.
     *
     *
     *
     * NOTE: This routing does not return Dao<T></T>, ID> because of casting issues if we are assigning it to a custom DAO.
     * Grumble.
     *
     */
    @Throws(SQLException::class)
    fun <D : Dao<T, *>, T> getDao(clazz: Class<T>): D {
        // special reflection fu is now handled internally by create dao calling the database type
        val dao = DaoManager.createDao(getConnectionSource(), clazz)
        return dao as D
    }

    /**
     * Get a RuntimeExceptionDao for our class. This uses the [DaoManager] to cache the DAO for future gets.
     *
     *
     *
     * NOTE: This routing does not return RuntimeExceptionDao<T></T>, ID> because of casting issues if we are assigning it to
     * a custom DAO. Grumble.
     *
     */
    fun <D : RuntimeExceptionDao<T, *>, T> getRuntimeExceptionDao(clazz: Class<T>): D {
        try {
            val dao = getDao(clazz)
            return RuntimeExceptionDao(dao) as D
        } catch (e: SQLException) {
            throw RuntimeException("Could not create RuntimeExcepitionDao for class $clazz", e)
        }

    }

    override fun toString(): String {
        return javaClass.getSimpleName() + "@" + Integer.toHexString(super.hashCode())
    }

    companion object {

        protected var logger = LoggerFactory.getLogger(OrmLiteSqliteOpenHelper::class.java!!)

        private fun openFileId(context: Context, fileId: Int): InputStream {
            return context.resources.openRawResource(fileId)
                ?: throw IllegalStateException("Could not find object config file with id $fileId")
        }

        private fun openFile(configFile: File?): InputStream? {
            try {
                return if (configFile == null) {
                    null
                } else {
                    FileInputStream(configFile)
                }
            } catch (e: FileNotFoundException) {
                throw IllegalArgumentException("Could not open config file " + configFile!!, e)
            }

        }
    }
}
