namespace ivaldez.Sql.IntegrationTests.Data
{
    public class SampleSurrogateKey
    {
        public int Pk { get; set; }
        public string TextValue { get; set; }
        public int? IntValue { get; set; }
        public decimal? DecimalValue { get; set; }
    }

    public class SampleSurrogateKeyWithDerivedColumns
    {
        public int Pk { get; set; }
        public string TextValue { get; set; }
        public int? IntValue { get; set; }
        public decimal? DecimalValue { get; set; }

        public string ExtraColumn => TextValue + ":" + (IntValue ?? 0);
    }
}