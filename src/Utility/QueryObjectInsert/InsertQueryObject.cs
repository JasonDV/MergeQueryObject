using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Jvaldez.Net.Sql.Utility.QueryObjectInsert
{
    public class InsertQueryObject<T>
    {
        public InsertQueryObject()
        {
            PrimaryKeyExpression = t => new object[] { };
            ColumnsToExcludeExpressionOnInsert = t => new object[] { };

            TargetPropertyNames = typeof(T)
                .GetProperties()
                .Select(x => x.Name)
                .ToArray();
        }
        
        public string[] TargetPropertyNames { get; }
        public bool KeepPrimaryKeyInInsertStatement { get; set; }
        public Expression<Func<T, object[]>> PrimaryKeyExpression { get; set; }
        public Expression<Func<T, object[]>> ColumnsToExcludeExpressionOnInsert { get; set; }
        public string[] GetPrimaryKey()
        {
            return GetPropertiesFromExpression(PrimaryKeyExpression);
        }
        public string[] GetColumnsToExcludeExpressionOnInsert()
        {
            var columnsToExcludeExpressionOnInsert = GetPropertiesFromExpression(ColumnsToExcludeExpressionOnInsert);

            return columnsToExcludeExpressionOnInsert;
        }

        public string InsertStatement(string tempTableName)
        {
            var retObj = new StringBuilder();
            var targetPropertyNames = 
                TargetPropertyNames
                    .Except(GetColumnsToExcludeExpressionOnInsert())
                    .ToArray();

            if (KeepPrimaryKeyInInsertStatement == false)
            {
                targetPropertyNames = targetPropertyNames
                    .Except(GetPrimaryKey())
                    .ToArray();
            }

            var names2 = targetPropertyNames.Select(x => "@" + x).ToArray();

            retObj.AppendLine($@"INSERT INTO {tempTableName}");
            retObj.AppendLine(@"(");

            retObj.AppendLine(string.Join(",", targetPropertyNames));

            retObj.AppendLine(@")");
            retObj.AppendLine(@"VALUES");
            retObj.AppendLine(@"(");
            retObj.AppendLine(string.Join(",", names2));
            retObj.AppendLine(@")");

            var insert = retObj.ToString();

            return insert;
        }

        private static string[] GetPropertiesFromExpression(
            Expression<Func<T, object[]>> primaryKeyExpression)
        {
            var retObj = new List<string>();

            var body = primaryKeyExpression.Body as NewArrayExpression;

            if (body == null)
            {
                throw new Exception("Merge statement must have join condition.");
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
}