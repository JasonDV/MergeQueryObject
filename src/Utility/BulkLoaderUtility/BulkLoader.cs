using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using FastMember;

namespace Jvaldez.Net.Sql.Utility.BulkLoaderUtility
{
    public interface IBulkLoader
    {
        void Insert<T>(
            string tableName,
            SqlConnection conn,
            bool keepIdentityColumnValue,
            IEnumerable<T> dataToInsert,
            int batchSize = 5000);

        BulkLoaderContext<T> InsertWithOptions<T>(
            string tableName,
            SqlConnection conn,
            bool keepIdentityColumnValue,
            IEnumerable<T> dataToInsert,
            int batchSize = 5000);

        void Insert<T>(
            string tableName,
            SqlConnection conn,
            bool keepIdentityColumnValue,
            IEnumerable<T> dataToInsert,
            List<string> propertiesToIgnore,
            Dictionary<string, string> renameFields,
            int batchSize = 5000);
    }

    public class BulkLoaderContext<T>
    {
        private readonly IBulkLoader _bulkLoader;
        private readonly SqlConnection _conn;
        private readonly IEnumerable<T> _dataToInsert;
        private readonly bool _keepIdentityColumnValue;
        private readonly Dictionary<string, string> _renameFields;
        private readonly string _tableName;

        private readonly List<string> _withoutMembers;
        private int _batchSize;

        public BulkLoaderContext(
            IBulkLoader bulkLoader,
            string tableName,
            SqlConnection conn,
            bool keepIdentityColumnValue,
            IEnumerable<T> dataToInsert,
            int batchSize)
        {
            _withoutMembers = new List<string>();
            _renameFields = new Dictionary<string, string>();
            _bulkLoader = bulkLoader;
            _tableName = tableName;
            _conn = conn;
            _keepIdentityColumnValue = keepIdentityColumnValue;
            _dataToInsert = dataToInsert;
            _batchSize = batchSize;
        }

        public BulkLoaderContext<T> With(Expression<Func<T, object>> expression, string newName)
        {
            var name = GetName(expression);

            _renameFields.Add(name, newName);

            return this;
        }

        public BulkLoaderContext<T> Without(Expression<Func<T, object>> expression)
        {
            var name = GetName(expression);

            _withoutMembers.Add(name);

            return this;
        }

        public BulkLoaderContext<T> Without(string name)
        {
            _withoutMembers.Add(name);

            return this;
        }

        private string GetName(Expression<Func<T, object>> expression)
        {
            var body = expression.Body as MemberExpression;

            if (body == null)
            {
                var ubody = (UnaryExpression) expression.Body;
                body = ubody.Operand as MemberExpression;
            }

            var name = body.Member.Name;

            return name;
        }

        public void Execute()
        {
            _bulkLoader.Insert(
                _tableName,
                _conn,
                _keepIdentityColumnValue,
                _dataToInsert,
                _withoutMembers,
                _renameFields,
                _batchSize);
        }

        public IReadOnlyDictionary<string, string> GetRenameRules()
        {
            return _renameFields;
        }

        public void SetBatchSize(int value)
        {
            _batchSize = value;
        }
    }

    public class BulkLoader : IBulkLoader
    {
        public BulkLoaderContext<T> InsertWithOptions<T>(
            string tableName,
            SqlConnection conn,
            bool keepIdentityColumnValue,
            IEnumerable<T> dataToInsert,
            int batchSize = 5000)
        {
            return new BulkLoaderContext<T>(
                this,
                tableName,
                conn,
                keepIdentityColumnValue,
                dataToInsert,
                batchSize);
        }

        public void Insert<T>(
            string tableName,
            SqlConnection conn,
            bool keepIdentityColumnValue,
            IEnumerable<T> dataToInsert,
            int batchSize = 5000)
        {
            Insert(
                tableName,
                conn,
                keepIdentityColumnValue,
                dataToInsert,
                new List<string>(),
                new Dictionary<string, string>(),
                batchSize);
        }

        public void Insert<T>(
            string tableName,
            SqlConnection conn,
            bool keepIdentityColumnValue,
            IEnumerable<T> dataToInsert,
            List<string> propertiesToIgnore,
            Dictionary<string, string> renameFields,
            int batchSize = 5000)
        {
            var targetProperties = GetTargetProperties<T>(propertiesToIgnore, renameFields);

            var options = SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.TableLock;

            if (keepIdentityColumnValue)
            {
                options = options | SqlBulkCopyOptions.KeepIdentity;
            }

            var batch = new List<T>(batchSize);

            foreach (var item in dataToInsert)
            {
                batch.Add(item);

                if (batch.Count >= batchSize)
                {
                    BulkCopy(tableName, conn, options, targetProperties, batch);
                    batch.Clear();
                }
            }

            if (batch.Any())
            {
                BulkCopy(tableName, conn, options, targetProperties, batch);
                batch.Clear();
            }
        }

        private static void BulkCopy<T>(string tableName, SqlConnection conn, SqlBulkCopyOptions options,
            TargetProperty[] targetProperties, IEnumerable<T> toInsert)
        {
            using (var bulkCopy = new SqlBulkCopy(conn, options, null))
            {
                var parameters = targetProperties.Select(x => x.OriginalName).ToArray();


                using (var reader = ObjectReader.Create(toInsert, parameters))
                {
                    foreach (var property in targetProperties)
                    {
                        bulkCopy.ColumnMappings.Add(property.OriginalName, property.Name);
                    }

                    bulkCopy.BulkCopyTimeout = 900;
                    bulkCopy.DestinationTableName = tableName;
                    bulkCopy.WriteToServer(reader);

                    bulkCopy.Close();
                }
            }
        }

        private static TargetProperty[] GetTargetProperties<T>(List<string> propertiesToIgnore,
            Dictionary<string, string> renameFields)
        {
            var ignoreProperties = new HashSet<string>(propertiesToIgnore);

            var targetProperties = typeof(T)
                .GetProperties()
                .Where(x => ignoreProperties.Contains(x.Name) == false)
                .Select(x =>
                {
                    var fieldName = x.Name;

                    if (renameFields.ContainsKey(fieldName))
                    {
                        fieldName = renameFields[fieldName];
                    }

                    return new TargetProperty
                    {
                        Name = fieldName,
                        OriginalName = x.Name,
                        Type = x.PropertyType,
                        PropertyInfo = x
                    };
                }).ToArray();

            return targetProperties;
        }
    }

    internal class TargetProperty
    {
        public string Name { get; set; }
        public Type Type { get; set; }
        public PropertyInfo PropertyInfo { get; set; }
        public string OriginalName { get; set; }
    }
}