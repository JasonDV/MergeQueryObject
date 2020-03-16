using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using ivaldez.Sql.SqlBulkLoader;

namespace ivaldez.Sql.SqlMergeQueryObject
{
    public class MergeRequest<T>
    {
        public MergeRequest()
        {
            PrimaryKeyExpression = t => new object[] { };
            ColumnsToExcludeExpressionOnInsert = t => new object[] { };
            ColumnsToExcludeExpressionOnUpdate = t => new object[] { };
            TargetDataSetFilter = null;
            SqlCommandTimeout = 0;
            WhenNotMatchedDeleteFieldName = "IsDeleted";

            OnMergeInsertActive = true;
            OnMergeUpdateActive = true;

            TargetPropertyNames = typeof(T)
                .GetProperties()
                .Select(x => x.Name)
                .ToArray();
        }

        /// <summary>
        ///     List of all public properties of the generic type
        /// </summary>
        public string[] TargetPropertyNames { get; }

        /// <summary>
        ///     The command timeout for the Merge statement at the server.
        ///     default value is 0
        /// </summary>
        public int SqlCommandTimeout { get; }

        /// <summary>
        ///     The data set to merge.
        /// </summary>
        public IEnumerable<T> DataToMerge { get; set; }

        /// <summary>
        ///     The name of the target table. The DataToMerge
        /// </summary>
        public string TargetTableName { get; set; }

        /// <summary>
        ///     If true, then create a temp table for the merge in the dbo schema.
        ///     If false, tempdb is used for the temp table.
        /// </summary>
        public bool UseRealTempTable { get; set; }

        /// <summary>
        ///     If true, the Primary Keys will be included in the update and insert statements.
        /// </summary>
        public bool KeepPrimaryKeyInInsertStatement { get; set; }

        /// <summary>
        ///     Indicates that the identity values should be preserved on insert.
        /// </summary>
        public bool KeepIdentityColumnValueOnInsert { get; set; }

        /// <summary>
        ///     Defines the behavior when a record is not matched in the target table.
        /// </summary>
        public DeleteBehavior WhenNotMatchedDeleteBehavior { get; set; }

        /// <summary>
        ///     The field name to use for soft deletes
        /// </summary>
        public string WhenNotMatchedDeleteFieldName { get; set; }

        /// <summary>
        /// Activates and Deactivates the ON MATCH part of the Merge
        /// </summary>
        public bool OnMergeUpdateActive { get; set; }

        /// <summary>
        /// Activates and Deactivates the NOT MATCHED part of the Merge
        /// </summary>
        public bool OnMergeInsertActive { get; set; }

        /// <summary>
        ///     An array representing the primary key of the target table
        /// </summary>
        public Expression<Func<T, object[]>> PrimaryKeyExpression { get; set; }

        /// <summary>
        ///     Special case array of fields to ignore for update
        /// </summary>
        public Expression<Func<T, object[]>> ColumnsToExcludeExpressionOnUpdate { get; set; }

        /// <summary>
        ///     Special case array of fields to ignore for insert
        /// </summary>
        public Expression<Func<T, object[]>> ColumnsToExcludeExpressionOnInsert { get; set; }

        /// <summary>
        ///     Used to constrain the working set of records in the target table.
        ///     Use this for efficiency.
        ///     When merging into a large table, create a target filter that narrows down the records to work with like a range of
        ///     the primary key or other indexed values.
        /// </summary>
        public Expression<Func<T, object>> TargetDataSetFilter { get; set; }

        /// <summary>
        ///     A delegate for logging information.
        /// </summary>
        public Action<string> InfoLogger { get; set; } = message => { };

        /// <summary>
        ///     A delegate for logging errors.
        /// </summary>
        public Action<string> ErrorLogger { get; set; } = message => { };

        public Action<BulkLoaderContext<T>> BulkLoaderOptions { get; set; } = context => { };
       
        /// <summary>
        ///     Get an array representing the primary key fields.
        /// </summary>
        /// <returns></returns>
        public string[] GetPrimaryKey()
        {
            return GetPropertiesFromExpression(PrimaryKeyExpression);
        }

        /// <summary>
        ///     Get an array of fields to exclude from the update
        /// </summary>
        /// <returns></returns>
        public string[] GetColumnsToExcludeExpressionOnUpdate()
        {
            var columnsToExcludeExpressionOnUpdate = GetPropertiesFromExpression(ColumnsToExcludeExpressionOnUpdate);

            return columnsToExcludeExpressionOnUpdate;
        }

        /// <summary>
        ///     Get an array of fields to exclude from the insert
        /// </summary>
        /// <returns></returns>
        public string[] GetColumnsToExcludeExpressionOnInsert()
        {
            var columnsToExcludeExpressionOnInsert = GetPropertiesFromExpression(ColumnsToExcludeExpressionOnInsert);

            return columnsToExcludeExpressionOnInsert;
        }

        private static string[] GetPropertiesFromExpression(
            Expression<Func<T, object[]>> primaryKeyExpression)
        {
            var retObj = new List<string>();

            var body = primaryKeyExpression.Body as NewArrayExpression;

            if (body == null)
            {
                throw new Exception("Must supply primary key expression.");
            }

            foreach (var bodyExpression in body.Expressions)
            {
                var unaryExpression = bodyExpression as UnaryExpression;
                var memberExpression = bodyExpression as MemberExpression;

                if (memberExpression != null)
                {
                    retObj.Add(memberExpression.Member.Name);
                }
                else if (unaryExpression != null)
                {
                    var unaryExpressionOperand = unaryExpression.Operand as MemberExpression;
                    retObj.Add(unaryExpressionOperand.Member.Name);
                }
            }

            return retObj.ToArray();
        }
    }

    public enum DeleteBehavior
    {
        /// <summary>
        /// No delete behavior. Nothing will be done.
        /// </summary>
        None = 0,
        /// <summary>
        /// Delete the record in the target table.
        /// </summary>
        Delete = 1,
        /// <summary>
        /// Mark the record for delete.
        /// The assumption is that there is a field called "IsDeleted" defined as a bit.
        /// </summary>
        MarkIsDelete = 2
    }
}