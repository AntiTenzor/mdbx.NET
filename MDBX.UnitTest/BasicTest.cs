using System;
using System.IO;
using System.Reflection;
using System.Text;
using System.Runtime.Serialization;
using System.Threading.Tasks;

using Xunit;


namespace MDBX.UnitTest
{
    public class BasicTest
    {
        /// <summary>
        /// Max number of databases
        /// </summary>
        private const int maxDatabases = 10;

        [Fact(DisplayName = "put / set / delete single key (strong type)")]
        public void Test1()
        {
            string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "mdbx");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            using (MdbxEnvironment env = new MdbxEnvironment())
            {
                env.SetMaxDatabases(maxDatabases) /* allow us to use a different db for testing */
                   .Open(path, EnvironmentFlag.NoTLS, Convert.ToInt32("666", 8));

                DatabaseOption optionCreate = DatabaseOption.Create /* needed to create a new db if not exists */
                    | DatabaseOption.IntegerKey /* optimized for fixed key */;

                // mdbx_put
                using (MdbxTransaction tran = env.BeginTransaction())
                {
                    MdbxDatabase db = tran.OpenDatabase("basic_op_test", optionCreate);
                    db.Put(10L, "ten");
                    db.Put(1000L, "thousand");
                    db.Put(1000000000L, "billion");
                    db.Put(1000000L, "million");
                    db.Put(100L, "hundred");
                    db.Put(1L, "one");
                    tran.Commit();
                }


                // Quote from XML-comment:
                // Option 'Create' is NOT allowed in a read-only transaction or a read-only environment.'
                // So we have to open existing database with special option set.
                DatabaseOption optionOpenExistingReadOnly = DatabaseOption.IntegerKey /* optimized for fixed key */;

                // mdbx_get
                using (MdbxTransaction tran = env.BeginTransaction(TransactionOption.ReadOnly))
                {
                    MdbxDatabase db = tran.OpenDatabase("basic_op_test", optionOpenExistingReadOnly);

                    string text = db.Get<long, string>(1000000L);
                    Assert.NotNull(text);
                    Assert.Equal("million", text);
                }

                // mdbx_del
                using (MdbxTransaction tran = env.BeginTransaction())
                {
                    MdbxDatabase db = tran.OpenDatabase("basic_op_test", optionOpenExistingReadOnly);
                    bool deleted = db.Del(100L);
                    Assert.True(deleted);
                    deleted = db.Del(100L);
                    Assert.False(deleted);
                    tran.Commit();
                }


                // mdbx_get
                using (MdbxTransaction tran = env.BeginTransaction(TransactionOption.ReadOnly))
                {
                    MdbxDatabase db = tran.OpenDatabase("basic_op_test", optionOpenExistingReadOnly);

                    string text = db.Get<long, string>(100L);
                    Assert.Null(text);
                }

                env.Close();
            }
        }

        [Fact(DisplayName = "put / set / delete single key (raw key)")]
        public void Test2()
        {
            string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "mdbx");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            using (MdbxEnvironment env = new MdbxEnvironment())
            {
                env.Open(path, EnvironmentFlag.NoTLS, Convert.ToInt32("666", 8));

                var putBytes = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());

                // mdbx_put
                using (MdbxTransaction tran = env.BeginTransaction())
                {
                    MdbxDatabase db = tran.OpenDatabase();
                    db.Put(putBytes, putBytes);
                    tran.Commit();
                }


                // mdbx_get
                using (MdbxTransaction tran = env.BeginTransaction(TransactionOption.ReadOnly))
                {
                    MdbxDatabase db = tran.OpenDatabase();

                    byte[] getBytes = db.Get(putBytes);
                    Assert.NotNull(getBytes);
                    Assert.Equal(putBytes.Length, getBytes.Length);
                    Assert.Equal(putBytes, getBytes);
                }

                // mdbx_del
                using (MdbxTransaction tran = env.BeginTransaction())
                {
                    MdbxDatabase db = tran.OpenDatabase();
                    bool deleted = db.Del(putBytes);
                    Assert.True(deleted);
                    deleted = db.Del(putBytes);
                    Assert.False(deleted);
                    tran.Commit();
                }


                env.Close();
            }
        }

        

        [Fact(DisplayName = "put / set / delete single key (custom serializer)")]
        public void Test3()
        {
            string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "mdbx");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            // register serializer for our custom type
            SerializerRegistry.Register(new BasicTest3PayloadSerializer());

            using (MdbxEnvironment env = new MdbxEnvironment())
            {
                env.Open(path, EnvironmentFlag.NoTLS, Convert.ToInt32("666", 8));


                // mdbx_put
                using (MdbxTransaction tran = env.BeginTransaction())
                {
                    MdbxDatabase db = tran.OpenDatabase();
                    db.Put("ana_key", new BasicTest3Payload() { Person = "Ana", Age = 50 } );
                    tran.Commit();
                }


                // mdbx_get
                using (MdbxTransaction tran = env.BeginTransaction(TransactionOption.ReadOnly))
                {
                    MdbxDatabase db = tran.OpenDatabase();

                    BasicTest3Payload payload = db.Get<string, BasicTest3Payload>("ana_key");
                    Assert.NotNull(payload);
                    Assert.Equal("Ana", payload.Person);
                    Assert.Equal(50, payload.Age);
                }



                env.Close();
            }
        }



        [Fact(DisplayName = "put / set single key (raw value)")]
        public void Test4()
        {
            string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "mdbx");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            using (MdbxEnvironment env = new MdbxEnvironment())
            {
                env.Open(path, EnvironmentFlag.NoTLS, Convert.ToInt32("666", 8));

                string key = Guid.NewGuid().ToString("N"); // some key
                byte[] value = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()); // some value in bytes


                // mdbx_get
                using (MdbxTransaction tran = env.BeginTransaction(TransactionOption.ReadOnly))
                {
                    MdbxDatabase db = tran.OpenDatabase();

                    byte[] getBytes = db.Get(key);
                    Assert.Null(getBytes);
                }


                // mdbx_put
                using (MdbxTransaction tran = env.BeginTransaction())
                {
                    MdbxDatabase db = tran.OpenDatabase();
                    db.Put(key, value);
                    tran.Commit();
                }


                // mdbx_get
                using (MdbxTransaction tran = env.BeginTransaction(TransactionOption.ReadOnly))
                {
                    MdbxDatabase db = tran.OpenDatabase();

                    byte[] getBytes = db.Get(key);
                    Assert.NotNull(getBytes);
                    Assert.Equal(value.Length, getBytes.Length);
                    Assert.Equal(value, getBytes);
                }


                env.Close();
            }
        }

        [Fact(DisplayName = "put / set EMPTY key")]
        public void TestEmptyKey()
        {
            string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "mdbx");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            using (MdbxEnvironment env = new MdbxEnvironment())
            {
                env.SetMaxDatabases(maxDatabases); /* allow us to use a different db for testing */
                env.Open(path, EnvironmentFlag.NoTLS, Convert.ToInt32("666", 8));

                byte[] key0 = new byte[0]; // EMPTY key for INTEGER-KEY DATABASE!
                byte[] value = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()); // some value in bytes



                // mdbx_put
                DatabaseOption optionCreate = DatabaseOption.Create /* needed to create a new db if not exists */
                    | DatabaseOption.IntegerKey /* optimized for fixed key */;

                using (MdbxTransaction tran = env.BeginTransaction())
                {
                    MdbxDatabase db = tran.OpenDatabase(name: "test_int_key", option: optionCreate);
                    // MDBX mdbx_put returned (-30781) - MDBX_BAD_VALSIZE: Invalid size or alignment of key or data for target database, either invalid subDB name
                    try
                    {
                        db.Put(key0, value);
                        throw new InvalidOperationException($"Engine must throw MdbxException with ErrorNumber:{MdbxCode.MDBX_BAD_VALSIZE} ==> MDBX_BAD_VALSIZE for EMPTY integer key.");
                    }
                    catch (MdbxException mEx)
                    {
                        if (mEx.ErrorNumber == MdbxCode.MDBX_BAD_VALSIZE)
                        {
                            // This is EXPECTED exception for EMPTY key
                        }
                        else
                        {
                            throw new InvalidOperationException($"Engine must throw MdbxException with ErrorNumber:{MdbxCode.MDBX_BAD_VALSIZE} ==> MDBX_BAD_VALSIZE for EMPTY integer key.");
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Engine must throw MdbxException with ErrorNumber:{MdbxCode.MDBX_BAD_VALSIZE} ==> MDBX_BAD_VALSIZE for EMPTY integer key.");
                    }
                    tran.Commit();
                }



                env.Close();
            }
        }

        [Fact(DisplayName = "put / set single-byte key")]
        public void TestByte1Key()
        {
            string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "mdbx");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            using (MdbxEnvironment env = new MdbxEnvironment())
            {
                env.SetMaxDatabases(maxDatabases); /* allow us to use a different db for testing */
                env.Open(path, EnvironmentFlag.NoTLS, Convert.ToInt32("666", 8));

                byte[] key1 = new byte[] { 17 }; // EMPTY key for INTEGER-KEY DATABASE!
                byte[] value = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()); // some value in bytes



                // mdbx_put
                DatabaseOption optionCreate = DatabaseOption.Create /* needed to create a new db if not exists */
                    | DatabaseOption.IntegerKey /* optimized for fixed key */;

                using (MdbxTransaction tran = env.BeginTransaction())
                {
                    MdbxDatabase db = tran.OpenDatabase(name: "test_int_key", option: optionCreate);
                    // MDBX mdbx_put returned (-30781) - MDBX_BAD_VALSIZE: Invalid size or alignment of key or data for target database, either invalid subDB name
                    try
                    {
                        db.Put(key1, value);
                        throw new InvalidOperationException($"Engine must throw MdbxException with ErrorNumber:{MdbxCode.MDBX_BAD_VALSIZE} ==> MDBX_BAD_VALSIZE for EMPTY integer key.");
                    }
                    catch (MdbxException mEx)
                    {
                        if (mEx.ErrorNumber == MdbxCode.MDBX_BAD_VALSIZE)
                        {
                            // This is EXPECTED exception for EMPTY key
                        }
                        else
                        {
                            throw new InvalidOperationException($"Engine must throw MdbxException with ErrorNumber:{MdbxCode.MDBX_BAD_VALSIZE} ==> MDBX_BAD_VALSIZE for EMPTY integer key.");
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Engine must throw MdbxException with ErrorNumber:{MdbxCode.MDBX_BAD_VALSIZE} ==> MDBX_BAD_VALSIZE for EMPTY integer key.");
                    }
                    tran.Commit();
                }



                env.Close();
            }
        }

        [Fact(DisplayName = "put / set Int32 key")]
        public void TestInt32Key()
        {
            string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "mdbx");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            using (MdbxEnvironment env = new MdbxEnvironment())
            {
                env.SetMaxDatabases(maxDatabases); /* allow us to use a different db for testing */
                env.Open(path, EnvironmentFlag.NoTLS, Convert.ToInt32("666", 8));

                Guid guid = Guid.NewGuid();
                string expected = guid.ToString();
                byte[] value = Encoding.UTF8.GetBytes(expected); // some value in bytes



                // mdbx_put
                DatabaseOption optionCreate = DatabaseOption.Create /* needed to create a new db if not exists */
                    | DatabaseOption.IntegerKey /* optimized for fixed key */;

                using (MdbxTransaction tran = env.BeginTransaction())
                {
                    MdbxDatabase db = tran.OpenDatabase(name: "test_int_key", option: optionCreate);

                    db.Empty();

                    db.Put(7, value);
                    tran.Commit();
                }



                // mdbx_get
                string actual = null;
                using (MdbxTransaction tran = env.BeginTransaction())
                {
                    MdbxDatabase db = tran.OpenDatabase(name: "test_int_key", option: optionCreate);
                    actual = db.Get(7, Encoding.UTF8);
                    tran.Commit();
                }

                Assert.Equal(expected.Length, actual.Length);
                Assert.Equal(expected, actual);



                env.Close();
            }
        }

        //[Fact(DisplayName = "put / set NULL key")]
        public void TestNullKey()
        {
            string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "mdbx");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            using (MdbxEnvironment env = new MdbxEnvironment())
            {
                env.Open(path, EnvironmentFlag.NoTLS, Convert.ToInt32("666", 8));

                //string key = Guid.NewGuid().ToString("N"); // some key
                byte[] value = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()); // some value in bytes



                // mdbx_put
                using (MdbxTransaction tran = env.BeginTransaction())
                {
                    MdbxDatabase db = tran.OpenDatabase();
                    // NRE from this call
                    db.Put(null, value);
                    tran.Commit();
                }



                env.Close();
            }
        }
    }
}
