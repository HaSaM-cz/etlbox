using System;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBox.ConnectionManager
{
    public static class ConnectionManagerSpecifics
    {
        #region Type

        public static ConnectionManagerType GetType(IConnectionManager connection)
        {
            if (connection is null)
                throw new System.ArgumentNullException(nameof(connection));
            if (connection.GetType() == typeof(SqlConnectionManager) ||
                connection.GetType() == typeof(SqlOdbcConnectionManager)
                )
                return ConnectionManagerType.SqlServer;
            else if (connection.GetType() == typeof(AccessOdbcConnectionManager))
                return ConnectionManagerType.Access;
            else if (connection.GetType() == typeof(AdomdConnectionManager))
                return ConnectionManagerType.Adomd;
            else if (connection.GetType() == typeof(SQLiteConnectionManager))
                return ConnectionManagerType.SQLite;
            else if (connection.GetType() == typeof(MySqlConnectionManager))
                return ConnectionManagerType.MySql;
            else if (connection.GetType() == typeof(PostgresConnectionManager))
                return ConnectionManagerType.Postgres;
            else return ConnectionManagerType.Unknown;
        }

        public static ConnectionManagerType Type(this IConnectionManager connection) => GetType(connection);

        #endregion

        #region Quotations

        public static string GetBeginQuotation(this ConnectionManagerType type)
        {
            if (type == ConnectionManagerType.SqlServer || type == ConnectionManagerType.Access)
                return @"[";
            else if (type == ConnectionManagerType.MySql)
                return @"`";
            else if (type == ConnectionManagerType.Postgres || type == ConnectionManagerType.SQLite)
                return @"""";
            else
                return string.Empty;
        }

        public static string GetEndQuotation(this ConnectionManagerType type)
        {
            if (type == ConnectionManagerType.SqlServer || type == ConnectionManagerType.Access)
                return @"]";
            else
                return GetBeginQuotation(type);
        }

        public static string AddQuotations(this ConnectionManagerType type, string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Value cannot be null or white space", nameof(name));
            string qb = type.GetBeginQuotation();
            string qe = type.GetEndQuotation();
            if (!name.StartsWith(qb))
                name = qb + name;
            if (!name.EndsWith(qe))
                name += qe;
            return name;
        }

        public static IEnumerable<string> AddQuotations(this ConnectionManagerType type, IEnumerable<string> names)
        {
            if (names is null)
                throw new ArgumentNullException(nameof(names));
            return names.Select(i => type.AddQuotations(i));
        }

        public static string AddQuotations(this IConnectionManager connection, string name) => connection.Type().AddQuotations(name);

        public static IEnumerable<string> AddQuotations(this IConnectionManager connection, IEnumerable<string> names) =>
            connection.Type().AddQuotations(names);

        public static string GetBeginQuotation(IConnectionManager connectionManager) => GetBeginQuotation(GetType(connectionManager));
        public static string GetEndQuotation(IConnectionManager connectionManager) => GetEndQuotation(GetType(connectionManager));

        #endregion

        #region SQL

        public static string SqlConcatColumns(this ConnectionManagerType type, IEnumerable<string> columnNames)
        {
            if (columnNames is null)
                throw new ArgumentNullException(nameof(columnNames));
            var columns = type.AddQuotations(columnNames).ToArray();
            return columns.Length switch
            {
                0 => string.Empty,
                1 => columns[0],
                _ => type == ConnectionManagerType.SQLite ?
                    $" {string.Join("||", columns)} " :
                    $"CONCAT( {string.Join(",", columns)} )"
            };
        }

        public static (string sql, ulong count) SqlIdIn(this ConnectionManagerType type, IEnumerable<string> idColumnNames, IEnumerable<string> ids)
        {
            if (ids is null)
                throw new ArgumentNullException(nameof(ids));
            string id = type.SqlConcatColumns(idColumnNames);
            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("At least one id column name is required", nameof(idColumnNames));
            ulong count = 0;
            string idsText = string.Join(",", ids.Select(i =>
            {
                count++;
                return $"'{i}'";
            }));
            if (string.IsNullOrWhiteSpace(idsText))
                throw new ArgumentException("At least one id is required", nameof(ids));
            string sql = $"{id} in ({idsText})";
            return (sql, count);
        }

        public static (string sql, bool supported) RowsAffected(this ConnectionManagerType type, string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("SQL must be non-(null or white space)", nameof(sql));
            switch (type)
            {
                // https://stackoverflow.com/questions/174143/counting-the-number-of-deleted-rows-in-a-sql-server-stored-procedure
                case ConnectionManagerType.SqlServer:
                    sql += "\nselect @@rowcount";
                    break;
                // https://stackoverflow.com/a/22546994/7821542
                case ConnectionManagerType.Postgres:
                    sql = $"with affected as ({sql} returning *) select count(*) from affected";
                    break;
                case ConnectionManagerType.Access:
                case ConnectionManagerType.Adomd:
                case ConnectionManagerType.SQLite:
                case ConnectionManagerType.MySql:
                case ConnectionManagerType.Unknown:
                default:
                    return (sql, false);
            }
            return (sql, true);
        }

        #endregion
    }
}
