#include "storage/default/default_handler.h"
#include "sql/parser/sql_parser.h"
#include "sql/executor/command_executor.h"

RC DefaultStorageStage::handle_sql(SQLStageEvent *sql_event)
{
  CommandExecutor      executor;
  const ParsedSqlNode &sql           = sql_event->get_sql_node();
  SessionEvent        *session_event = sql_event->session_event();

  char response[1024];
  RC   rc = RC::SUCCESS;

  switch (sql.flag) {
    case SCF_DROP_TABLE: {
      const DropTable &drop_table = sql.sstr[sql.q_size - 1].drop_table;
      rc                          = handler_->drop_table(current_db, drop_table.relation_name);
      snprintf(response, sizeof(response), "%s\n", rc == RC::SUCCESS ? "SUCCESS" : "FAILURE");
      break;
    }
    // ���� case ���Լ�������
    default: rc = RC::UNIMPLEMENTED; snprintf(response, sizeof(response), "Unsupported SQL command.\n");
  }

  session_event->set_response(response);
  return rc;
}