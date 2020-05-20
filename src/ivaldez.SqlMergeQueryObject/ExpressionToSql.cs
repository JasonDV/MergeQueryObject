using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace ivaldez.Sql.SqlMergeQueryObject
{
    public static class SqlFunction
    {
        public static bool In<S>(S leftSide, string rightSide)
        {
            return true;
        }
    }


    public class ExpressionToSql
    {
        public string GenerateWhereClause<T>(Expression<Func<T, object>> expression)
        {
            if (expression == null)
                return "";

            return "WHERE " + Process(expression.Body);
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
            else if (expression is MethodCallExpression
                && (((MethodCallExpression) expression).Method).DeclaringType?.Name == "SqlFunction")
            {
                leftText += "[" + ((MemberExpression)((MethodCallExpression)expression).Arguments[0]).Member.Name + "]";
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
            else if (expression is MethodCallExpression
                     && (((MethodCallExpression) expression).Method).DeclaringType?.Name == "SqlFunction")
            {
                var methodCallExpression = (MethodCallExpression)expression;
                if (methodCallExpression.Arguments[1] is NewArrayExpression)
                {
                    var memberExpression =  (NewArrayExpression)(methodCallExpression.Arguments[1]);
                    rightText += "(" + string.Join(",", memberExpression.Expressions) + ")";
                }
                else if (methodCallExpression.Arguments[1].GetType().Name == "FieldExpression" )
                {
                    var expressionArg = methodCallExpression.Arguments[1];

                    var memberName =  ((FieldInfo)expressionArg.GetType()
                        .GetProperty("Member")
                        .GetValue(methodCallExpression.Arguments[1]))
                        .Name;

                    var constantExpression = (ConstantExpression)expressionArg.GetType()
                        .GetProperty("Expression")
                        .GetValue(methodCallExpression.Arguments[1]);
                    
                    var constantExpressionValue = constantExpression.Value;
                    var value = constantExpressionValue.GetType()
                        .GetField(memberName).GetValue(constantExpressionValue);

                    rightText += "(" + value + ")";
                }
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
            var operationName = body.NodeType.ToString();

            if (body is UnaryExpression)
            {
                operationName = ((UnaryExpression) body).Operand.NodeType.ToString();
            }
            else if (body is MethodCallExpression)
            {
                operationName = ((MethodCallExpression) body).Method.Name;
            }

            if (operationName == "GreaterThan")
            {
                operation = ">";
            }
            else if (operationName == "GreaterThanOrEqual")
            {
                operation = ">=";
            }
            else if (operationName == "LessThanOrEqual")
            {
                operation = "<=";
            }
            else if (operationName == "LessThan")
            {
                operation = "<";
            }
            else if (operationName == "AndAlso")
            {
                operation = "AND";
            }
            else if (operationName == "Equal")
            {
                operation = "=";
            }
            else if (operationName == "NotEqual")
            {
                operation = "<>";
            }
            else if (operationName == "OrElse")
            {
                operation = "OR";
            }
            else if (operationName == "In")
            {
                operation = "IN";
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
                value = "[" + ((MemberExpression) expression).Member.Name + "]";
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