using System.Data.SqlClient;

namespace ivaldez.Sql.SqlMergeQueryObject
{
    /// <summary>
    /// MergeQueryObject
    /// An abstraction for the SQL Merge statement with many options and efficiencies.
    /// Author: Jason Valdez
    /// Repository: https://github.com/JasonDV/MergeQueryObject
    /// </summary>
    public interface IMergeQueryObject
    {
        void Merge<T>(
            SqlConnection connection,
            MergeRequest<T> request);
    }
}