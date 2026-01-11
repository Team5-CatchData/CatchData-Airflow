from typing import Optional

import requests
from airflow.utils.context import Context


class SlackNotifier:
    """
    Slack Webhook을 사용하여 알림을 전송하는 유틸리티 클래스

    Airflow Task의 성공/실패 알림, 커스텀 메시지 전송 등에 사용됩니다.

    :param webhook_url: Slack Webhook URL
    :type webhook_url: str
    :param username: Slack 봇 이름 (선택사항)
    :type username: str, optional
    :param icon_emoji: Slack 봇 아이콘 (선택사항)
    :type icon_emoji: str, optional

    **Example:**

    .. code-block:: python

        # 기본 사용
        notifier = SlackNotifier(webhook_url='https://hooks.slack.com/...')
        notifier.send_message('작업 완료!')

        # Airflow 콜백으로 사용
        with DAG(...) as dag:
            task = MyOperator(
                task_id='my_task',
                on_success_callback=SlackNotifier.success_callback,
                on_failure_callback=SlackNotifier.failure_callback,
            )
    """

    def __init__(
        self,
        webhook_url: str,
        username: str = "Airflow Bot",
        icon_emoji: str = ":robot_face:"
    ):
        """
        SlackNotifier 초기화

        :param webhook_url: Slack Webhook URL
        :type webhook_url: str
        :param username: Slack 봇 이름
        :type username: str
        :param icon_emoji: Slack 봇 아이콘
        :type icon_emoji: str
        """
        self.webhook_url = webhook_url
        self.username = username
        self.icon_emoji = icon_emoji

    def send_message(
        self,
        message: str,
        color: Optional[str] = None,
        title: Optional[str] = None
    ) -> bool:
        """
        Slack으로 메시지를 전송합니다.

        :param message: 전송할 메시지
        :type message: str
        :param color: 메시지 색상 (good, warning, danger 또는 hex 코드)
        :type color: str, optional
        :param title: 메시지 제목
        :type title: str, optional
        :return: 전송 성공 여부
        :rtype: bool
        """
        if not self.webhook_url:
            return False

        try:
            payload = {
                "username": self.username,
                "icon_emoji": self.icon_emoji,
            }

            # 단순 텍스트 메시지
            if not color and not title:
                payload["text"] = message
            else:
                # Attachment 형식 (색상, 제목 포함)
                attachment = {
                    "text": message,
                    "mrkdwn_in": ["text"]
                }
                if color:
                    attachment["color"] = color
                if title:
                    attachment["title"] = title

                payload["attachments"] = [attachment]

            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            return True

        except Exception as e:
            print(f"Slack 알림 전송 실패: {e}")
            return False

    def send_success(self, message: str, title: str = "Success") -> bool:
        """
        성공 메시지를 전송합니다 (녹색).

        :param message: 전송할 메시지
        :type message: str
        :param title: 메시지 제목
        :type title: str
        :return: 전송 성공 여부
        :rtype: bool
        """
        return self.send_message(message, color="good", title=title)

    def send_warning(self, message: str, title: str = "Warning") -> bool:
        """
        경고 메시지를 전송합니다 (노란색).

        :param message: 전송할 메시지
        :type message: str
        :param title: 메시지 제목
        :type title: str
        :return: 전송 성공 여부
        :rtype: bool
        """
        return self.send_message(message, color="warning", title=title)

    def send_error(self, message: str, title: str = "Error") -> bool:
        """
        에러 메시지를 전송합니다 (빨간색).

        :param message: 전송할 메시지
        :type message: str
        :param title: 메시지 제목
        :type title: str
        :return: 전송 성공 여부
        :rtype: bool
        """
        return self.send_message(message, color="danger", title=title)

    @staticmethod
    def _format_context_info(context: Context) -> str:
        """
        Airflow Context에서 유용한 정보를 포맷팅합니다.

        :param context: Airflow 실행 컨텍스트
        :type context: Context
        :return: 포맷팅된 컨텍스트 정보
        :rtype: str
        """
        dag_id = context.get('dag').dag_id
        task_id = context.get('task_instance').task_id
        execution_date = context.get('execution_date')
        log_url = context.get('task_instance').log_url

        info = (
            f"*DAG*: `{dag_id}`\n"
            f"*Task*: `{task_id}`\n"
            f"*Execution Date*: {execution_date}\n"
            f"*Log URL*: {log_url}"
        )
        return info

    @staticmethod
    def success_callback(context: Context):
        """
        Task 성공 시 호출되는 콜백 함수

        Airflow Task의 on_success_callback으로 사용할 수 있습니다.

        :param context: Airflow 실행 컨텍스트
        :type context: Context

        **Example:**

        .. code-block:: python

            task = MyOperator(
                task_id='my_task',
                on_success_callback=SlackNotifier.success_callback,
            )
        """
        from airflow.sdk import Variable

        try:
            webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
            if not webhook_url:
                return

            notifier = SlackNotifier(webhook_url=webhook_url)
            context_info = SlackNotifier._format_context_info(context)

            message = (
                f"Task가 성공적으로 완료되었습니다.\n\n"
                f"{context_info}"
            )

            notifier.send_success(message, title="Task Success")

        except Exception as e:
            print(f"Success callback 실행 실패: {e}")

    @staticmethod
    def failure_callback(context: Context):
        """
        Task 실패 시 호출되는 콜백 함수

        Airflow Task의 on_failure_callback으로 사용할 수 있습니다.

        :param context: Airflow 실행 컨텍스트
        :type context: Context

        **Example:**

        .. code-block:: python

            task = MyOperator(
                task_id='my_task',
                on_failure_callback=SlackNotifier.failure_callback,
            )
        """
        from airflow.sdk import Variable

        try:
            webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
            if not webhook_url:
                return

            notifier = SlackNotifier(webhook_url=webhook_url)
            context_info = SlackNotifier._format_context_info(context)

            # 에러 정보 추출
            exception = context.get('exception')
            error_message = str(exception) if exception else "Unknown error"

            message = (
                f"Task가 실패했습니다.\n\n"
                f"{context_info}\n\n"
                f"*Error*:\n```{error_message}```"
            )

            notifier.send_error(message, title="Task Failed")

        except Exception as e:
            print(f"Failure callback 실행 실패: {e}")

    @staticmethod
    def retry_callback(context: Context):
        """
        Task 재시도 시 호출되는 콜백 함수

        Airflow Task의 on_retry_callback으로 사용할 수 있습니다.

        :param context: Airflow 실행 컨텍스트
        :type context: Context

        **Example:**

        .. code-block:: python

            task = MyOperator(
                task_id='my_task',
                on_retry_callback=SlackNotifier.retry_callback,
            )
        """
        from airflow.sdk import Variable

        try:
            webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
            if not webhook_url:
                return

            notifier = SlackNotifier(webhook_url=webhook_url)
            context_info = SlackNotifier._format_context_info(context)

            ti = context.get('task_instance')
            try_number = ti.try_number
            max_tries = ti.max_tries

            message = (
                f"Task가 재시도 중입니다.\n\n"
                f"{context_info}\n\n"
                f"*Retry*: {try_number}/{max_tries}"
            )

            notifier.send_warning(message, title="Task Retry")

        except Exception as e:
            print(f"Retry callback 실행 실패: {e}")


# Convenience 함수들
def send_slack_message(webhook_url: str, message: str) -> bool:
    """
    간단하게 Slack 메시지를 전송하는 헬퍼 함수

    :param webhook_url: Slack Webhook URL
    :type webhook_url: str
    :param message: 전송할 메시지
    :type message: str
    :return: 전송 성공 여부
    :rtype: bool

    **Example:**

    .. code-block:: python

        from plugins.utils.slack_notifier import send_slack_message

        send_slack_message(
            webhook_url='https://hooks.slack.com/...',
            message='Hello from Airflow!'
        )
    """
    notifier = SlackNotifier(webhook_url=webhook_url)
    return notifier.send_message(message)


def send_slack_success(webhook_url: str, message: str) -> bool:
    """
    간단하게 Slack 성공 메시지를 전송하는 헬퍼 함수

    :param webhook_url: Slack Webhook URL
    :type webhook_url: str
    :param message: 전송할 메시지
    :type message: str
    :return: 전송 성공 여부
    :rtype: bool
    """
    notifier = SlackNotifier(webhook_url=webhook_url)
    return notifier.send_success(message)


def send_slack_error(webhook_url: str, message: str) -> bool:
    """
    간단하게 Slack 에러 메시지를 전송하는 헬퍼 함수

    :param webhook_url: Slack Webhook URL
    :type webhook_url: str
    :param message: 전송할 메시지
    :type message: str
    :return: 전송 성공 여부
    :rtype: bool
    """
    notifier = SlackNotifier(webhook_url=webhook_url)
    return notifier.send_error(message)
