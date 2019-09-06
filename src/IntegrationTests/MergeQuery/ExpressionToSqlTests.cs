using System;
using FluentAssertions;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class ExpressionToSqlTests
    {
        public class ExpressionToSqlTestsDto
        {
            public DateTime TimeStamp { get; set; }
            public string TextValue { get; internal set; }
            public decimal DecimalValue { get; internal set; }
            public int IntValue { get; internal set; }
        }

        [Fact]
        public void ShouldParseMultipleOperation()
        {
            var stuff = new ExpressionToSql();

            var sql1 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => w.IntValue > 10 && w.TextValue == "Jason");
            sql1.Should().Be("WHERE ((IntValue > 10) AND (TextValue = 'Jason'))");

            var sql2b = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w =>
                w.IntValue > 10 || w.TextValue == "Jason" && w.DecimalValue == 100m);
            sql2b.Should().Be("WHERE ((IntValue > 10) OR ((TextValue = 'Jason') AND (DecimalValue = 100)))");

            var sql2a = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w =>
                (w.IntValue > 10 || w.TextValue == "Jason") && w.DecimalValue == 100m);
            sql2a.Should().Be("WHERE (((IntValue > 10) OR (TextValue = 'Jason')) AND (DecimalValue = 100))");

            var sql2 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w =>
                w.IntValue > 10 && (w.TextValue == "Jason" || w.DecimalValue == 100m));
            sql2.Should().Be("WHERE ((IntValue > 10) AND ((TextValue = 'Jason') OR (DecimalValue = 100)))");
        }

        [Fact]
        public void ShouldParseSingleOperation()
        {
            var stuff = new ExpressionToSql();

            var sql1aa = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => w.IntValue >= 10);
            sql1aa.Should().Be("WHERE (IntValue >= 10)");

            var sql1bb = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => w.IntValue <= 10);
            sql1bb.Should().Be("WHERE (IntValue <= 10)");

            var sql1 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => w.IntValue > 10);
            sql1.Should().Be("WHERE (IntValue > 10)");

            var sql1a = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => w.IntValue < 10);
            sql1a.Should().Be("WHERE (IntValue < 10)");

            var sql2 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => 10 > w.IntValue);
            sql2.Should().Be("WHERE (10 > IntValue)");

            var sql3 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => w.TextValue == "10");
            sql3.Should().Be("WHERE (TextValue = '10')");

            var sql4 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => "10" == w.TextValue);
            sql4.Should().Be("WHERE ('10' = TextValue)");

            var sql5 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => w.TextValue != "10");
            sql5.Should().Be("WHERE (TextValue <> '10')");

            var sql6 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => "10" != w.TextValue);
            sql6.Should().Be("WHERE ('10' <> TextValue)");
        }

        [Fact]
        public void ShouldParseSingleOperationWithVariable()
        {
            var stuff = new ExpressionToSql();

            var abc = 10;
            var sql1 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => w.IntValue > abc);
            sql1.Should().Be("WHERE (IntValue > 10)");

            var sql1a = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => abc < w.IntValue);
            sql1a.Should().Be("WHERE (10 < IntValue)");

            var eee = 10;
            var abc2 = eee;
            var sql2 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => w.IntValue > abc2);
            sql2.Should().Be("WHERE (IntValue > 10)");
        }

        [Fact]
        public void ShouldParseSqlDate()
        {
            var stuff = new ExpressionToSql();

            var start = new DateTime(2019, 7, 1);
            var end = new DateTime(2019, 7, 2);
            var sql1 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => w.TimeStamp >= start && w.TimeStamp <= end);
            sql1.Should().Be("WHERE ((TimeStamp >= '2019-07-01') AND (TimeStamp <= '2019-07-02'))");

            var sql2 = stuff.GenerateWhereClause<ExpressionToSqlTestsDto>(w => w.TimeStamp > start || w.TimeStamp < end);
            sql2.Should().Be("WHERE ((TimeStamp > '2019-07-01') OR (TimeStamp < '2019-07-02'))");
        }
    }
}