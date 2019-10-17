using System;
using System.Data.Common;
using MySql.Data.MySqlClient;
using NHibernate.AdoNet;
using NHibernate.Driver;
using NHibernate.Engine;

namespace NHibernate.MySqlConnector.Driver
{
	public class MySqlConnectorDriver : DriverBase, IEmbeddedBatcherFactoryProvider
	{
        /// <summary>
        /// Provides a database driver for MySQL using the MySqlConnector library.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Please check <see href="https://github.com/mysql-net/MySqlConnector">MySqlConnector></see>
        /// repository for more information.
        /// </para>
        /// </remarks>
        public MySqlConnectorDriver()
        {
        }

		/// <summary>
		/// MySqlConnector uses named parameters in the sql.
		/// </summary>
		/// <value><see langword="true" /> - MySql uses <c>?</c> in the sql.</value>
		public override bool UseNamedPrefixInSql => true;

		/// <summary></summary>
		public override bool UseNamedPrefixInParameter => true;

		/// <summary>
		/// MySqlConnector use the <c>?</c> to locate parameters in sql.
		/// </summary>
		/// <value><c>?</c> is used to locate parameters in sql.</value>
		public override string NamedPrefix => "?";

        /// <summary>
        /// The MySqlConnector driver does NOT support more than 1 open DbDataReader
        /// with only 1 DbConnection.
        /// </summary>
        /// <value><see langword="false" /> - it is not supported.</value>
        public override bool SupportsMultipleOpenReaders => false;

        /// <summary>
        /// MySqlConnector supports preparing of commands https://github.com/mysql-net/MySqlConnector/pull/534
        /// </summary>
        /// <value><see langword="true" /> - it is supported.</value>
        protected override bool SupportsPreparingCommands => true;

        public override DbConnection CreateConnection()
        {
            return new MySqlConnection();
        }

        public override DbCommand CreateCommand()
        {
            return new MySqlCommand();
        }

        public override IResultSetsCommand GetResultSetsCommand(ISessionImplementor session)
		{
			return new BasicResultSetsCommand(session);
		}

		public override bool SupportsMultipleQueries => true;

		public override bool RequiresTimeSpanForTime => true;

		// As of v5.7, lower dates may "work" but without guarantees.
		// https://dev.mysql.com/doc/refman/5.7/en/datetime.html
		/// <inheritdoc />
		public override DateTime MinDate => new DateTime(1000, 1, 1);

		System.Type IEmbeddedBatcherFactoryProvider.BatcherFactoryClass => typeof(GenericBatchingBatcherFactory);
	}
}
