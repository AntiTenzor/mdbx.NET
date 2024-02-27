using System;
using System.Text;
using System.Collections.Generic;

namespace MDBX
{
    public static class MdbxEnvironmentExtensions
    {
        /// <summary>
        /// This method packs all magic to the single call:
        /// - Begin transaction;
        /// - Open database;
        /// - Put;
        /// - Commit;
        /// 
        /// It throws an exception in all cases except Success
        /// </summary>
        /// <param name="env">existing and OPEN environment</param>
        /// <param name="dbName">database name (equivalent of table name in relational DB)</param>
        /// <param name="key">integer key</param>
        /// <param name="text">string value to put to the DB (NOT NULL!)</param>
        /// <param name="encoding">encoding to convert string to byte array</param>
        /// <param name="transFlags">transaction flags (default: Unspecific)</param>
        /// <param name="dbFlags">database flags (default: Create + IntegerKey)</param>
        /// <param name="putOptions">special put options (default: Unspecific)</param>
        /// <exception cref="MdbxException">in case of any problems</exception>
        public static void Put(this MdbxEnvironment env,
            string dbName,
            int key, string text, Encoding encoding,
            TransactionOption transFlags = TransactionOption.Unspecific,
            DatabaseOption dbFlags = DatabaseOption.Create | DatabaseOption.IntegerKey,
            PutOption putOptions = PutOption.Unspecific)
        {
            using (var tx = env.BeginTransaction(flags: transFlags))
            using (var db = tx.OpenDatabase(dbName, dbFlags))
            {
                db.Put(key, text, encoding, option: putOptions);
                
                // Commit closes transaction
                tx.Commit();
            }
        }

        /// <summary>
        /// This method packs all magic to the single call:
        /// - Begin transaction;
        /// - Open database;
        /// - TryGet;
        /// 
        /// It throws an exception in really critical cases
        /// </summary>
        /// <param name="env">existing and OPEN environment</param>
        /// <param name="dbName">database name (equivalent of table name in relational DB)</param>
        /// <param name="key">integer key</param>
        /// <param name="encoding">Encoding to convert byte array to string (NOT NULL!).</param>
        /// <param name="text">A string containing the value found in the database, if it exists.</param>
        /// <param name="transFlags">transaction flags (default: ReadOnly)</param>
        /// <param name="dbFlags">database flags (default: IntegerKey)</param>
        /// <returns>true, in case of success; false if key not found; throws an exception in case of critical errors</returns>
        /// <exception cref="ArgumentNullException">if encoding is null</exception>
        public static bool TryGet(this MdbxEnvironment env,
            string dbName,
            int key, Encoding encoding, out string text,
            TransactionOption transFlags = TransactionOption.ReadOnly,
            DatabaseOption dbFlags = DatabaseOption.IntegerKey)
        {
            using (var tx = env.BeginTransaction(flags: transFlags))
            using (var db = tx.OpenDatabase(dbName, dbFlags))
            {
                int errCode = db.Get(key, encoding, out text);
                bool res = (errCode == MdbxCode.MDBX_SUCCESS);
                return res;
            }
        }
    }
}
