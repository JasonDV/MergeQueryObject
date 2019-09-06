using System.Linq;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlBulkLoader;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTests
    {
        public class MergeQueryObjectTestHelper
        {
            public MergeQueryObjectTestHelper()
            {
                TestingDatabaseService = new TestingDatabaseService();
                TestingDatabaseService.CreateTestDatabase();
                DataGateway = new TestingDataGateway(TestingDatabaseService, new MergeQueryObject(new BulkLoader(), new ExpressionToSql()));
            }

            public TestingDataGateway DataGateway { get; set; }

            public TestingDatabaseService TestingDatabaseService { get; set; }
        }

        [Fact]
        public void ShouldAllowForParitalUpdateWithPartialObject()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateCompositeKeyTable();

            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleCompositeKeyDto().ToArray();

            var firstDto = databaseDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);
            firstDto.IsDeleted.Should().BeFalse();

            //**

            var dtos2 = new[]
            {
                new SampleCompositeKeyPartialUpdateDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ_Partial"
                }
            };


            var request2 = new MergeRequest<SampleCompositeKeyPartialUpdateDto>
            {
                DataToMerge = dtos2,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None,
                OnMergeUpdateOnly = true
            };

            dataGateway.Merge(request2);

            var databaseDtos2 = dataGateway.GetAllSampleCompositeKeyDto().ToArray();

            var firstDto2 = databaseDtos2.First(x => x.Pk1 == 1);
            firstDto2.Pk2.Should().Be("A");
            firstDto2.TextValue.Should().Be("JJ_Partial");
            firstDto2.IntValue.Should().Be(100);
            firstDto2.DecimalValue.Should().Be(100.99m);
            firstDto2.IsDeleted.Should().BeFalse();
        }

        [Fact]
        public void ShouldConsrainByFilterClause()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateCompositeKeyTable();

            dataGateway.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 3,
                Pk2 = "B",
                TextValue = "AA",
                IntValue = 1,
                DecimalValue = 1
            });

            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleCompositeKeyDto
                {
                    Pk1 = 2,
                    Pk2 = "B",
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                TargetDataSetFilter = t => t.Pk1 >= 1 && t.Pk1 <= 2,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleCompositeKeyDto().ToArray();

            databaseDtos.Length.Should().Be(3);

            var firstDto = databaseDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = databaseDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }

        [Fact]
        public void ShouldCreateSampleTable()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;

            dataGateway.DropTable();
            dataGateway.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleSurrogateKey
                {
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = false
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleSurrogateKey().ToArray();

            var firstDto = databaseDtos.First(x => x.TextValue == "JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = databaseDtos.First(x => x.TextValue == "ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }

        [Fact]
        public void ShouldDeleteWhenNotMatched()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateCompositeKeyTable();

            dataGateway.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 3,
                Pk2 = "B",
                TextValue = "AA",
                IntValue = 1,
                DecimalValue = 1
            });

            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleCompositeKeyDto
                {
                    Pk1 = 2,
                    Pk2 = "B",
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleCompositeKeyDto().ToArray();

            databaseDtos.Length.Should().Be(2);

            var firstDto = databaseDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = databaseDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }

        [Fact]
        public void ShouldExecuteWithTempTable()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = false
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleSurrogateKey().ToArray();

            var firstDto = databaseDtos.First(x => x.TextValue == "JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);
        }

        [Fact]
        public void ShouldIncludePrimaryKeyInUpdate()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateCompositeKeyTable();

            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleCompositeKeyDto
                {
                    Pk1 = 2,
                    Pk2 = "B",
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleCompositeKeyDto().ToArray();

            var firstDto = databaseDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = databaseDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }

        [Fact]
        public void ShouldKeepIdentityColumnsWhenFlagged()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    Pk = 100,
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleSurrogateKey
                {
                    Pk = 200,
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = true,
                KeepIdentityColumnValueOnInsert = true
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleSurrogateKey().ToArray();

            var firstDto = databaseDtos.First(x => x.Pk == 100);
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = databaseDtos.First(x => x.Pk == 200);
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }

        [Fact]
        public void ShouldMarkForDeleteWhenNotMatched()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateCompositeKeyTable();

            dataGateway.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 3,
                Pk2 = "B",
                TextValue = "AA",
                IntValue = 1,
                DecimalValue = 1
            });

            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleCompositeKeyDto
                {
                    Pk1 = 2,
                    Pk2 = "B",
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.MarkIsDelete
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleCompositeKeyDto().ToArray();

            databaseDtos.Length.Should().Be(3);

            var firstDto = databaseDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);
            firstDto.IsDeleted.Should().BeFalse();

            var secondDto = databaseDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
            secondDto.IsDeleted.Should().BeFalse();

            var thirdDto = databaseDtos.First(x => x.Pk1 == 3);
            thirdDto.Pk2.Should().Be("B");
            thirdDto.TextValue.Should().Be("AA");
            thirdDto.IntValue.Should().Be(1);
            thirdDto.DecimalValue.Should().Be(1);
            thirdDto.IsDeleted.Should().BeTrue();
        }

        [Fact]
        public void ShouldMergeDtoWithDerivedColumns()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKeyWithDerivedColumns
                {
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleSurrogateKeyWithDerivedColumns
                {
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleSurrogateKeyWithDerivedColumns>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                ColumnsToExcludeExpressionOnUpdate = t => new object[] {t.ExtraColumn},
                ColumnsToExcludeExpressionOnInsert = t => new object[] {t.ExtraColumn},
                KeepPrimaryKeyInInsertStatement = false
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleSurrogateKey().ToArray();

            var firstDto = databaseDtos.First(x => x.TextValue == "JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = databaseDtos.First(x => x.TextValue == "ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }

        [Fact]
        public void ShouldMergeWhenBulkLoadUsesRenamedFields()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKeyDifferentNamesDto
                {
                    Pk = 100,
                    TextValueExtra = "JJ",
                    IntValueExtra = 100,
                    DecimalValueExtra = 100.99m
                },
                new SampleSurrogateKeyDifferentNamesDto
                {
                    Pk = 200,
                    TextValueExtra = "ZZ",
                    IntValueExtra = 999,
                    DecimalValueExtra = 123.45m
                }
            };

            var request = new MergeRequest<SampleSurrogateKeyDifferentNamesDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = false,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete,
                OnMergeUpdateOnly = false,
                BulkLoaderOptions =
                    t => t.With(c => c.TextValueExtra, "TextValue")
                        .With(c => c.IntValueExtra, "IntValue")
                        .With(c => c.DecimalValueExtra, "DecimalValue")
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleSurrogateKey().ToArray();

            var firstDto = databaseDtos.First(x => x.TextValue == "JJ");
            firstDto.Pk.Should().BeGreaterThan(0);
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = databaseDtos.First(x => x.TextValue == "ZZ");
            secondDto.Pk.Should().BeGreaterThan(0);
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }

        [Fact]
        public void ShouldMergeWhenBulkLoadUsesRenamedPrimaryKey()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;

            dataGateway.DropTable();
            dataGateway.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKeyDifferentNamePrimaryKeyDto
                {
                    PkPrimaryKey = 100,
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleSurrogateKeyDifferentNamePrimaryKeyDto
                {
                    PkPrimaryKey = 200,
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleSurrogateKeyDifferentNamePrimaryKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.PkPrimaryKey},
                KeepPrimaryKeyInInsertStatement = true,
                KeepIdentityColumnValueOnInsert = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None,
                OnMergeUpdateOnly = false,
                BulkLoaderOptions =
                    t => t.With(c => c.PkPrimaryKey, "Pk")
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleSurrogateKey().ToArray();

            var firstDto = databaseDtos.First(x => x.TextValue == "JJ");
            firstDto.Pk.Should().Be(100);
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = databaseDtos.First(x => x.TextValue == "ZZ");
            secondDto.Pk.Should().Be(200);
            secondDto.Pk.Should().BeGreaterThan(0);
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }

        [Fact]
        public void ShouldNotDeleteIfNotInTargetFilter()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateCompositeKeyTable();

            dataGateway.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 3,
                Pk2 = "B",
                TextValue = "AA",
                IntValue = 1,
                DecimalValue = 1
            });

            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleCompositeKeyDto
                {
                    Pk1 = 2,
                    Pk2 = "B",
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                TargetDataSetFilter = t => t.Pk1 >= 1 && t.Pk1 <= 2,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleCompositeKeyDto().ToArray();

            databaseDtos.Length.Should().Be(3);

            var firstDto = databaseDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = databaseDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }

        [Fact]
        public void ShouldNotFailWhenAllKeysArePartOfPrimary()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleSurrogateKey
                {
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.TextValue, t.IntValue, t.DecimalValue},
                ColumnsToExcludeExpressionOnInsert = t => new object[] {t.Pk},
                ColumnsToExcludeExpressionOnUpdate = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = true
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleSurrogateKey().ToArray();

            var firstDto = databaseDtos.First(x => x.TextValue == "JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = databaseDtos.First(x => x.TextValue == "ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }

        [Fact]
        public void ShouldUpdateFieldsWithMerge()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleSurrogateKey
                {
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = false
            };

            dataGateway.Merge(request);

            var firstSetOfInsertedDtos = dataGateway.GetAllSampleSurrogateKey().ToArray();

            var firstInsertDto = firstSetOfInsertedDtos.First(x => x.TextValue == "JJ");
            firstInsertDto.IntValue.Should().Be(100);
            firstInsertDto.DecimalValue.Should().Be(100.99m);

            var secondInsertDto = firstSetOfInsertedDtos.First(x => x.TextValue == "ZZ");
            secondInsertDto.IntValue.Should().Be(999);
            secondInsertDto.DecimalValue.Should().Be(123.45m);

            firstInsertDto.TextValue = "1";
            firstInsertDto.IntValue = 2;
            firstInsertDto.DecimalValue = 3;

            secondInsertDto.TextValue = "2";
            secondInsertDto.IntValue = 3;
            secondInsertDto.DecimalValue = 4;

            request.DataToMerge = firstSetOfInsertedDtos;

            dataGateway.Merge(request);

            var secondSetOfUpdateDtos = dataGateway.GetAllSampleSurrogateKey().ToArray();

            var firstUpdateDto = secondSetOfUpdateDtos.First(x => x.TextValue == "1");
            firstUpdateDto.IntValue.Should().Be(2);
            firstUpdateDto.DecimalValue.Should().Be(3);

            var secondUpdateDto = secondSetOfUpdateDtos.First(x => x.TextValue == "2");
            secondUpdateDto.IntValue.Should().Be(3);
            secondUpdateDto.DecimalValue.Should().Be(4);
        }

        [Fact]
        public void ShouldUpdateOnlyAndDeleteWhenFlagged()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateCompositeKeyTable();


            dataGateway.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 1,
                Pk2 = "A",
                TextValue = "JJ",
                IntValue = 100,
                DecimalValue = 100.99m
            });

            dataGateway.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 2,
                Pk2 = "B",
                TextValue = "ZZ",
                IntValue = 999,
                DecimalValue = 123.45m
            });

            dataGateway.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 3,
                Pk2 = "B",
                TextValue = "AA",
                IntValue = 1,
                DecimalValue = 1
            });


            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "zz",
                    IntValue = 999,
                    DecimalValue = 999m
                },
                new SampleCompositeKeyDto
                {
                    Pk1 = 4,
                    Pk2 = "C",
                    TextValue = "CC",
                    IntValue = 666,
                    DecimalValue = 666.66m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete,
                OnMergeUpdateOnly = true
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleCompositeKeyDto().ToArray();

            databaseDtos.Length.Should().Be(1);

            var firstDto = databaseDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("zz");
            firstDto.IntValue.Should().Be(999);
            firstDto.DecimalValue.Should().Be(999);
        }

        [Fact]
        public void ShouldUpdateOnlyWhenFlagged()
        {
            var helper = new MergeQueryObjectTestHelper();
            var dataGateway = helper.DataGateway;
            dataGateway.DropTable();
            dataGateway.CreateCompositeKeyTable();


            dataGateway.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 1,
                Pk2 = "A",
                TextValue = "JJ",
                IntValue = 100,
                DecimalValue = 100.99m
            });

            dataGateway.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 2,
                Pk2 = "B",
                TextValue = "ZZ",
                IntValue = 999,
                DecimalValue = 123.45m
            });

            dataGateway.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 3,
                Pk2 = "B",
                TextValue = "AA",
                IntValue = 1,
                DecimalValue = 1
            });


            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "zz",
                    IntValue = 999,
                    DecimalValue = 999m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = true,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None,
                OnMergeUpdateOnly = true
            };

            dataGateway.Merge(request);

            var databaseDtos = dataGateway.GetAllSampleCompositeKeyDto().ToArray();

            var firstDto = databaseDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("zz");
            firstDto.IntValue.Should().Be(999);
            firstDto.DecimalValue.Should().Be(999);

            var secondDto = databaseDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);

            var thirdDto = databaseDtos.First(x => x.Pk1 == 3);
            thirdDto.Pk2.Should().Be("B");
            thirdDto.TextValue.Should().Be("AA");
            thirdDto.IntValue.Should().Be(1);
            thirdDto.DecimalValue.Should().Be(1);
        }
    }
}