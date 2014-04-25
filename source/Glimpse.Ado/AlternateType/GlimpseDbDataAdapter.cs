using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Glimpse.Core.Message;
using Glimpse.Ado.Message;
using Glimpse.Ado.AlternateType;

namespace Glimpse.Ado.AlternateType
{
    public class GlimpseDbDataAdapter : DbDataAdapter
    {
        public GlimpseDbDataAdapter(DbDataAdapter innerDataAdapter)
        {
            InnerDataAdapter = innerDataAdapter;
        }

        public override bool ReturnProviderSpecificTypes
        {
            get { return InnerDataAdapter.ReturnProviderSpecificTypes; }
            set { InnerDataAdapter.ReturnProviderSpecificTypes = value; }
        }

        public override int UpdateBatchSize
        {
            get { return InnerDataAdapter.UpdateBatchSize; }
            set { InnerDataAdapter.UpdateBatchSize = value; }
        }

        private DbDataAdapter InnerDataAdapter { get; set; }

        public override int Fill(DataSet dataSet)
        {
            if (SelectCommand != null)
            {
                var typedCommand = SelectCommand as GlimpseDbCommand;
                if (typedCommand != null)
                {
                    InnerDataAdapter.SelectCommand = typedCommand.Inner;

                    var result = 0;
                    var commandId = Guid.NewGuid();

                    var timer = typedCommand.LogCommandSeed();
                    typedCommand.LogCommandStart(commandId, timer);
                    try
                    {
                        result = InnerDataAdapter.Fill(dataSet);
                    }
                    catch (Exception exception)
                    {
                        typedCommand.LogCommandError(commandId, timer, exception, "ExecuteDbDataReader");
                        throw;
                    }
                    finally
                    {
                        typedCommand.LogCommandEnd(commandId, timer, result, "ExecuteDbDataReader");
                    }

                    return result;
                }

                InnerDataAdapter.SelectCommand = SelectCommand;
            }

            return InnerDataAdapter.Fill(dataSet);
        }

        public override DataTable[] FillSchema(DataSet dataSet, SchemaType schemaType)
        {
            if (SelectCommand != null)
            {
                InnerDataAdapter.SelectCommand = RetrieveBaseType(SelectCommand);
            }

            return InnerDataAdapter.FillSchema(dataSet, schemaType);
        }

        public override IDataParameter[] GetFillParameters()
        {
            return InnerDataAdapter.GetFillParameters();
        }

        public override bool ShouldSerializeAcceptChangesDuringFill()
        {
            return InnerDataAdapter.ShouldSerializeAcceptChangesDuringFill();
        }

        public override bool ShouldSerializeFillLoadOption()
        {
            return InnerDataAdapter.ShouldSerializeFillLoadOption();
        }

        public override string ToString()
        {
            return InnerDataAdapter.ToString();
        }

        public override int Update(DataSet dataSet)
        {
            if (UpdateCommand != null)
            {
                InnerDataAdapter.UpdateCommand = RetrieveBaseType(UpdateCommand);
            }

            if (InsertCommand != null)
            {
                InnerDataAdapter.InsertCommand = RetrieveBaseType(InsertCommand);
            }

            if (DeleteCommand != null)
            {
                InnerDataAdapter.DeleteCommand = RetrieveBaseType(DeleteCommand);
            }

            return InnerDataAdapter.Update(dataSet);
        }

        protected override int Update(DataRow[] dataRows, DataTableMapping tableMapping)
        {
            GlimpseDbCommand accountCommand = null;

            if (UpdateCommand != null)
            {
                InnerDataAdapter.UpdateCommand = RetrieveBaseType(UpdateCommand);
                accountCommand = accountCommand ?? UpdateCommand as GlimpseDbCommand;
            }

            if (InsertCommand != null)
            {
                InnerDataAdapter.InsertCommand = RetrieveBaseType(InsertCommand);
                accountCommand = accountCommand ?? InsertCommand as GlimpseDbCommand;
            }

            if (DeleteCommand != null)
            {
                InnerDataAdapter.DeleteCommand = RetrieveBaseType(DeleteCommand);
                accountCommand = accountCommand ?? DeleteCommand as GlimpseDbCommand;
            }

            if (accountCommand != null && accountCommand.MessageBroker != null)
            {
                var commandId = Guid.NewGuid();
                var timer = accountCommand.LogCommandSeed();

                IList<CommandExecutedParamater> parameters = null;
                if (accountCommand.Parameters.Count > 0)
                {
                    parameters = new List<CommandExecutedParamater>();
                    if (dataRows.Length > 0)
                    {
                        parameters.Add(new CommandExecutedParamater { Name = "#rows", Value = dataRows.Length, Type = DbType.Int32.ToString(), Size = 0 });
                        Dictionary<string, CommandExecutedParamater> map = new Dictionary<string, CommandExecutedParamater>(parameters.Count);
                        foreach (IDbDataParameter parameter in accountCommand.Parameters)
                        {
                            var parameterName = parameter.ParameterName;
                            if (!parameterName.StartsWith("@"))
                            {
                                parameterName = "@" + parameterName;
                            }
                            var name = parameter.SourceColumn ?? parameter.ParameterName;
                            map[name] = new CommandExecutedParamater { Name = parameterName, Value = "?", Type = parameter.DbType.ToString(), Size = parameter.Size };
                        }
                        var names = new List<string>(map.Keys);
                        for (int i = 0; i < dataRows.Length; i++)
                        {
                            var row = dataRows[i];
                            var deleted = row.RowState == DataRowState.Deleted;
                            foreach (var name in names)
                            {
                                var val = deleted ? row[name, DataRowVersion.Original] : row[name];
                                var template = map[name];
                                parameters.Add(new CommandExecutedParamater { Name = template.Name + "#" + i, Value = Support.TranslateValue(val), Size = template.Size, Type = template.Type });
                            }
                        }
                    }
                    else
                    {
                        foreach (IDbDataParameter parameter in accountCommand.Parameters)
                        {
                            var parameterName = parameter.ParameterName;
                            if (!parameterName.StartsWith("@"))
                            {
                                parameterName = "@" + parameterName;
                            }
                            parameters.Add(new CommandExecutedParamater { Name = parameterName, Value = Support.GetParameterValue(parameter), Type = parameter.DbType.ToString(), Size = parameter.Size });
                        }
                    }
                }
                accountCommand.MessageBroker.Publish(
                    new CommandExecutedMessage(accountCommand.InnerConnection.ConnectionId, commandId, accountCommand.CommandText, parameters, accountCommand.InnerCommand.Transaction != null, false)
                    .AsTimedMessage(timer));

                int result = 0;
                try
                {
                    result = InnerDataAdapter.Update(dataRows);
                }
                catch (Exception exception)
                {
                    accountCommand.LogCommandError(commandId, timer, exception, "DbDataAdapter:Update");
                    throw;
                }
                accountCommand.LogCommandEnd(commandId, timer, result, "DbDataAdapter:Update");
                return result;
            }
            return InnerDataAdapter.Update(dataRows);
        }

        protected override void Dispose(bool disposing)
        {
            InnerDataAdapter.Dispose();
        }

        private DbCommand RetrieveBaseType(DbCommand command)
        {
            var typedCommand = command as GlimpseDbCommand;
            return typedCommand.Inner ?? command;
        }
    }
}
