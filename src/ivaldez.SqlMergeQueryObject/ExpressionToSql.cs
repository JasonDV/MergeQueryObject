using System;
using System.Linq.Expressions;

namespace ivaldez.Sql.SqlMergeQueryObject
{
    public class ExpressionToSql
    {
        public string GenerateWhereClause<T>(Expression<Func<T, object>> expression)
        {
            if (expression == null)
                return "";

            var unaryExpression = (UnaryExpression) expression.Body;

            return "WHERE " + Process(unaryExpression);
        }

        private static string Process(Expression expression)
        {
            object leftText = "";
            object rightText = "";

            if (expression is UnaryExpression
                && ((UnaryExpression) expression).Operand is BinaryExpression
                && ((BinaryExpression) ((UnaryExpression) expression).Operand).Left is BinaryExpression)
            {
                leftText += Process(((BinaryExpression) ((UnaryExpression) expression).Operand).Left);
            }
            else if (expression is BinaryExpression
                     && ((BinaryExpression) expression).Left is BinaryExpression)
            {
                leftText += Process(((BinaryExpression) expression).Left);
            }
            else
            {
                if (expression is UnaryExpression)
                {
                    var aaa = expression as UnaryExpression;
                    leftText = ProcessLeft((BinaryExpression) aaa.Operand);
                }
                else if (expression is BinaryExpression)
                {
                    var aaa = expression as BinaryExpression;
                    leftText = ProcessLeft(aaa);
                }
            }

            var operation = GetOperation(expression);

            if (expression is UnaryExpression
                && ((UnaryExpression) expression).Operand is BinaryExpression
                && ((BinaryExpression) ((UnaryExpression) expression).Operand).Left is BinaryExpression)
            {
                rightText += Process(((BinaryExpression) ((UnaryExpression) expression).Operand).Right);
            }
            else if (expression is BinaryExpression
                     && ((BinaryExpression) expression).Right is BinaryExpression)
            {
                rightText += Process(((BinaryExpression) expression).Right);
            }
            else
            {
                if (expression is UnaryExpression)
                {
                    var bbb = expression as UnaryExpression;
                    rightText = ProcessRight((BinaryExpression) bbb.Operand);
                }
                else if (expression is BinaryExpression)
                {
                    var bbb = expression as BinaryExpression;
                    rightText = ProcessRight(bbb);
                }
            }

            return $"({leftText} {operation} {rightText})";
        }

        private static string GetOperation(Expression body)
        {
            var operation = "";
            var operationName = body.NodeType;

            if (body is UnaryExpression)
            {
                operationName = ((UnaryExpression) body).Operand.NodeType;
            }

            if (operationName.ToString() == "GreaterThan")
            {
                operation = ">";
            }
            else if (operationName.ToString() == "GreaterThanOrEqual")
            {
                operation = ">=";
            }
            else if (operationName.ToString() == "LessThanOrEqual")
            {
                operation = "<=";
            }
            else if (operationName.ToString() == "LessThan")
            {
                operation = "<";
            }
            else if (operationName.ToString() == "AndAlso")
            {
                operation = "AND";
            }
            else if (operationName.ToString() == "Equal")
            {
                operation = "=";
            }
            else if (operationName.ToString() == "NotEqual")
            {
                operation = "<>";
            }
            else if (operationName.ToString() == "OrElse")
            {
                operation = "OR";
            }

            return operation;
        }

        private static object ProcessLeft(BinaryExpression left)
        {
            var expression = GetExpression(left.Left);
            var value = GetValue(expression);

            return value;
        }

        private static object ProcessRight(BinaryExpression right)
        {
            var expression = GetExpression(right.Right);
            var value = GetValue(expression);

            return value;
        }

        private static object GetValue(Expression expression)
        {
            object value = "";

            if (expression is ConstantExpression)
            {
                value = ((ConstantExpression) expression).Value;

                if (value is string)
                {
                    value = $"'{value}'";
                }
            }
            else if (expression is MemberExpression
                     && (expression as MemberExpression).Expression is ConstantExpression)
            {
                var memberExpression = expression as MemberExpression;
                var constantExpression = memberExpression.Expression as ConstantExpression;
                var constantExpressionValue = constantExpression.Value;
                var memberName = memberExpression.Member.Name;
                value = constantExpressionValue.GetType()
                    .GetField(memberName).GetValue(constantExpressionValue);

                if (value is DateTime)
                {
                    value = $"'{value:yyyy-MM-dd}'";
                }
            }
            else if (expression is MemberExpression)
            {
                value = ((MemberExpression) expression).Member.Name;
            }

            return value;
        }

        private static Expression GetExpression(Expression right)
        {
            Expression expression = null;

            if (right is UnaryExpression)
            {
                expression = ((UnaryExpression) right).Operand;
            }

            if (right is MemberExpression)
            {
                expression = (MemberExpression) right;
            }

            if (right is ConstantExpression)
            {
                expression = (ConstantExpression) right;
            }

            return expression;
        }
    }
}