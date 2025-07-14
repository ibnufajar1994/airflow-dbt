from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import traceback as tb

def slack_notifier(context):
    """
    Send a detailed Slack notification when a task event occurs.
    """
    slack_icon = "red_circle"  # icon to indicate a failure state

    # extract task and execution details from context
    ti = context.get('task_instance')
    task_state = ti.state
    task_id = ti.task_id
    dag_id = ti.dag_id
    exec_date = context.get('execution_date')
    log_url = ti.log_url

    # capture exception info and formatted traceback
    exception = context.get('exception') or 'No exception'
    traceback_str = (
        "".join(tb.format_exception(type(exception), exception, exception.__traceback__))
        if context.get('exception') else 'No traceback'
    )

    # construct Slack message payload
    slack_msg = f"""
:{slack_icon}: *Task {task_state}*
*DAG*: `{dag_id}`
*Task*: `{task_id}`
*Execution Time*: `{exec_date}`
*Log URL*: `{log_url}`
*Exception*: ```{exception}```
*Traceback*: ```{traceback_str}```
"""

    # initialize and execute the Slack webhook operator
    SlackWebhookOperator(
        task_id='slack_notification',
        slack_webhook_conn_id='slack_webhook',  # connection ID defined in Airflow
        message=slack_msg,
        channel='pacflight-notifications'
    ).execute(context=context)
