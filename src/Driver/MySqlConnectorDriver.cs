using System;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using NHibernate.AdoNet;
using NHibernate.AdoNet.Util;
using NHibernate.Driver;
using NHibernate.Engine;
using NHibernate.Exceptions;

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
        /// <value><see langword="false" /> - it is supported.</value>
        protected override bool SupportsPreparingCommands => true;

        public override DbConnection CreateConnection()
        {
            return new MySqlConnection();
        }

        public override DbCommand CreateCommand()
        {
            return new MySqlCommand();
        }

        public override IResultSetsCommand GetResultSetsCommand(Engine.ISessionImplementor session)
		{
			return new BasicResultSetsCommand(session);
		}

		public override bool SupportsMultipleQueries => true;

		public override bool RequiresTimeSpanForTime => true;

		// As of v5.7, lower dates may "work" but without guarantees.
		// https://dev.mysql.com/doc/refman/5.7/en/datetime.html
		/// <inheritdoc />
		public override DateTime MinDate => new DateTime(1000, 1, 1);

		System.Type IEmbeddedBatcherFactoryProvider.BatcherFactoryClass => typeof(MySqlConnectorBatchingBatcherFactory);
	}

    public class MySqlConnectorSqlCommandSet : IDisposable
    {
        private MySqlDataAdapter mySqlDataAdapter;
        private MySqlBatch mySqlBatch;
        private int countOfCommands;

        public MySqlConnectorSqlCommandSet(int batchSize)
        {
            mySqlDataAdapter = new MySqlDataAdapter { UpdateBatchSize = batchSize };
            mySqlBatch = new MySqlBatch();
        }

        public void Append(DbCommand command)
        {
            mySqlBatch.BatchCommands.Add(new MySqlBatchCommand(command.CommandText));
            countOfCommands++;
        }

        public void Dispose()
        {
            mySqlBatch.Dispose();
            mySqlDataAdapter.Dispose();
        }

        public int ExecuteNonQuery()
        {
            if (CountOfCommands == 0)
            {
                return 0;
            }

            return mySqlBatch.ExecuteNonQuery();
        }

        public async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            if (CountOfCommands == 0)
            {
                return 0;
            }

            return await mySqlBatch.ExecuteNonQueryAsync(cancellationToken);
        }

        public int CountOfCommands => countOfCommands;
    }

    public class MySqlConnectorBatchingBatcherFactory : IBatcherFactory
    {
        public virtual IBatcher CreateBatcher(ConnectionManager connectionManager, IInterceptor interceptor)
        {
            return new MySqlConnectorBatchingBatcher(connectionManager, interceptor);
        }
    }

    public class MySqlConnectorBatchingBatcher : AbstractBatcher
    {
        private int batchSize;
        private int totalExpectedRowsAffected;
        private MySqlConnectorSqlCommandSet currentBatch;
        private StringBuilder currentBatchCommandsLog;

        public MySqlConnectorBatchingBatcher(ConnectionManager connectionManager, IInterceptor interceptor)
            : base(connectionManager, interceptor)
        {
            batchSize = Factory.Settings.AdoBatchSize;
            currentBatch = CreateConfiguredBatch();

            //we always create this, because we need to deal with a scenario in which
            //the user change the logging configuration at runtime. Trying to put this
            //behind an if(log.IsDebugEnabled) will cause a null reference exception 
            //at that point.
            currentBatchCommandsLog = new StringBuilder().AppendLine("Batch commands:");
        }

        public override int BatchSize
        {
            get { return batchSize; }
            set { batchSize = value; }
        }

        protected override int CountOfStatementsInCurrentBatch
        {
            get { return currentBatch.CountOfCommands; }
        }

        public override void AddToBatch(IExpectation expectation)
        {
            // MySql batcher cannot be initiated if a data reader is still open: check them.
            if (CountOfStatementsInCurrentBatch == 0)
                CheckReaders();

            totalExpectedRowsAffected += expectation.ExpectedRowCount;
            var batchUpdate = CurrentCommand;
            Prepare(batchUpdate);
            Driver.AdjustCommand(batchUpdate);
            string lineWithParameters = null;
            var sqlStatementLogger = Factory.Settings.SqlStatementLogger;
            if (sqlStatementLogger.IsDebugEnabled || Log.IsDebugEnabled())
            {
                lineWithParameters = sqlStatementLogger.GetCommandLineWithParameters(batchUpdate);
                var formatStyle = sqlStatementLogger.DetermineActualStyle(FormatStyle.Basic);
                lineWithParameters = formatStyle.Formatter.Format(lineWithParameters);
                currentBatchCommandsLog.Append("command ")
                    .Append(currentBatch.CountOfCommands)
                    .Append(":")
                    .AppendLine(lineWithParameters);
            }
            if (Log.IsDebugEnabled())
            {
                Log.Debug("Adding to batch:{0}", lineWithParameters);
            }
            currentBatch.Append(batchUpdate);

            if (currentBatch.CountOfCommands >= batchSize)
            {
                DoExecuteBatch(batchUpdate);
            }
        }

        public override async Task AddToBatchAsync(IExpectation expectation, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // MySql batcher cannot be initiated if a data reader is still open: check them.
            if (CountOfStatementsInCurrentBatch == 0)
                CheckReaders();

            totalExpectedRowsAffected += expectation.ExpectedRowCount;
            var batchUpdate = CurrentCommand;
            Prepare(batchUpdate);
            Driver.AdjustCommand(batchUpdate);
            string lineWithParameters = null;
            var sqlStatementLogger = Factory.Settings.SqlStatementLogger;
            if (sqlStatementLogger.IsDebugEnabled || Log.IsDebugEnabled())
            {
                lineWithParameters = sqlStatementLogger.GetCommandLineWithParameters(batchUpdate);
                var formatStyle = sqlStatementLogger.DetermineActualStyle(FormatStyle.Basic);
                lineWithParameters = formatStyle.Formatter.Format(lineWithParameters);
                currentBatchCommandsLog.Append("command ")
                    .Append(currentBatch.CountOfCommands)
                    .Append(":")
                    .AppendLine(lineWithParameters);
            }
            if (Log.IsDebugEnabled())
            {
                Log.Debug("Adding to batch:{0}", lineWithParameters);
            }
            currentBatch.Append(batchUpdate);

            if (currentBatch.CountOfCommands >= batchSize)
            {
                await DoExecuteBatchAsync(batchUpdate, cancellationToken).ConfigureAwait(false);
            }
        }

        protected override void DoExecuteBatch(DbCommand ps)
        {
            try
            {
                Log.Debug("Executing batch");
                CheckReaders();
                if (Factory.Settings.SqlStatementLogger.IsDebugEnabled)
                {
                    Factory.Settings.SqlStatementLogger.LogBatchCommand(currentBatchCommandsLog.ToString());
                }

                int rowsAffected;
                try
                {
                    rowsAffected = currentBatch.ExecuteNonQuery();
                }
                catch (DbException e)
                {
                    throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, e, "could not execute batch command.");
                }

                Expectations.VerifyOutcomeBatched(totalExpectedRowsAffected, rowsAffected, ps);
            }
            finally
            {
                ClearCurrentBatch();
            }
        }

        protected override async Task DoExecuteBatchAsync(DbCommand ps, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                Log.Debug("Executing batch");
                CheckReaders();
                if (Factory.Settings.SqlStatementLogger.IsDebugEnabled)
                {
                    Factory.Settings.SqlStatementLogger.LogBatchCommand(currentBatchCommandsLog.ToString());
                }

                int rowsAffected;
                try
                {
                    rowsAffected = await currentBatch.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (DbException e)
                {
                    throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, e, "could not execute batch command.");
                }

                Expectations.VerifyOutcomeBatched(totalExpectedRowsAffected, rowsAffected, ps);
            }
            finally
            {
                ClearCurrentBatch();
            }
        }

        private MySqlConnectorSqlCommandSet CreateConfiguredBatch()
        {
            return new MySqlConnectorSqlCommandSet(batchSize);
        }

        private void ClearCurrentBatch()
        {
            currentBatch.Dispose();
            totalExpectedRowsAffected = 0;
            currentBatch = CreateConfiguredBatch();

            if (Factory.Settings.SqlStatementLogger.IsDebugEnabled)
            {
                currentBatchCommandsLog = new StringBuilder().AppendLine("Batch commands:");
            }
        }

        public override void CloseCommands()
        {
            base.CloseCommands();

            try
            {
                ClearCurrentBatch();
            }
            catch (Exception e)
            {
                // Prevent exceptions when clearing the batch from hiding any original exception
                // (We do not know here if this batch closing occurs after a failure or not.)
                Log.Warn(e, "Exception clearing batch");
            }
        }

        protected override void Dispose(bool isDisposing)
        {
            base.Dispose(isDisposing);
            // Prevent exceptions when closing the batch from hiding any original exception
            // (We do not know here if this batch closing occurs after a failure or not.)
            try
            {
                currentBatch.Dispose();
            }
            catch (Exception e)
            {
                Log.Warn(e, "Exception closing batcher");
            }
        }
    }
}
