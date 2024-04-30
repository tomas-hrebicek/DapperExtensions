using Dapper;
using System;
using System.Collections.Generic;
using System.Data;

namespace DapperExtensions
{
    public static class DapperExtensions
    {
        public static IEnumerable<T> Query<T>(this IDbConnection cnn, string sql, Action<IDataRecord, T> configuration, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var reader = cnn.ExecuteReader(sql, param, transaction, commandTimeout, commandType))
            {
                List<T> items = new List<T>();
                var rowParser = reader.GetRowParser<T>();

                while (reader.Read())
                {
                    T item = rowParser.Invoke(reader);
                    configuration?.Invoke(reader, item);
                    items.Add(item);
                }

                return items;
            }
        }

        public static IEnumerable<T> Query<T>(this IDbConnection cnn, string sql, Func<IDataRecord, T> parseRow, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            if (parseRow is null)
            {
                throw new ArgumentNullException(nameof(parseRow));
            }

            List<T> items = new List<T>(); 
            
            using (var reader = cnn.ExecuteReader(sql, param, transaction, commandTimeout, commandType))
            {
                while (reader.Read())
                {
                    items.Add(parseRow.Invoke(reader));
                }
            }

            return items;
        }

        public static T QueryFirst<T>(this IDbConnection cnn, string sql, Action<IDataRecord, T> configuration, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var reader = cnn.ExecuteReader(sql, param, transaction, commandTimeout, commandType))
            {
                if (reader.Read())
                {
                    T item = reader.GetRowParser<T>().Invoke(reader);
                    configuration?.Invoke(reader, item);
                    return item;
                }
                else
                {
                    throw new InvalidOperationException();
                }
            }
        }

        public static T QueryFirst<T>(this IDbConnection cnn, string sql, Func<IDataRecord, T> parseRow, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            using (var reader = cnn.ExecuteReader(sql, param, transaction, commandTimeout, commandType))
            {
                if (reader.Read())
                {
                    return parseRow.Invoke(reader);
                }
                else
                {
                    throw new InvalidOperationException();
                }
            }
        }

        public static T QueryFirstOrDefault<T>(this IDbConnection cnn, string sql, Action<IDataRecord, T> configuration, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            T item = default;

            using (var reader = cnn.ExecuteReader(sql, param, transaction, commandTimeout, commandType))
            {
                if (reader.Read())
                {
                    item = reader.GetRowParser<T>().Invoke(reader);
                    configuration?.Invoke(reader, item);
                    return item;
                }
            }

            return item;
        }

        public static T QueryFirstOrDefault<T>(this IDbConnection cnn, string sql, Func<IDataRecord, T> parseRow, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            T item = default;

            using (var reader = cnn.ExecuteReader(sql, param, transaction, commandTimeout, commandType))
            {
                if (reader.Read())
                {
                    item = parseRow.Invoke(reader);
                }
            }

            return item;
        }
    }
}