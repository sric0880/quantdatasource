import logging
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd

smtp_api = None
from_email = "26189324@qq.com"
password = "iwnxmnyczgzkbjba"
smtp_address = "smtp.qq.com"
port = 465

# to_emails = ["40192741@qq.com", "532978024@qq.com"]

to_my_emails = ["532978024@qq.com"]


def init_email_api():
    global smtp_api
    try:
        smtp_api = smtplib.SMTP_SSL(smtp_address, port, timeout=120)
        smtp_api.login(from_email, password)
    except smtplib.SMTPHeloError:
        logging.error("SMTP_SSL 服务器没有正确回复 HELO 问候")
    except smtplib.SMTPAuthenticationError:
        logging.error("SMTP_SSL 服务器不接受所提供的用户名/密码组合")
    except smtplib.SMTPNotSupportedError:
        logging.error("SMTP_SSL 服务器不支持 AUTH 命令")
    except smtplib.SMTPException:
        logging.error("SMTP_SSL 未找到适当的认证方法")
    except Exception as e:
        logging.error("SMTP_SSL " + str(e))


def send_email(to_emails, title="", message="", html=""):
    if smtp_api is None:
        logging.error("ERROR: 邮箱系统没有登录成功")
        return
    email = MIMEMultipart()
    email["Subject"] = title
    email["From"] = from_email
    email["To"] = ", ".join(to_emails)
    if message:
        email.attach(MIMEText(message))
    if html:
        email.attach(MIMEText(html, "html"))

    try:
        smtp_api.sendmail(from_email, to_emails, email.as_string())  # 发送邮件
        logging.info(f"Email send to {to_emails} successfully")
    except (smtplib.SMTPDataError, smtplib.SMTPSenderRefused) as e:
        logging.error(
            f"{datetime.now()} 邮件 to {to_emails} 发送失败: code {e.smtp_code}  error  {e.smtp_error.decode()}"
        )
    except smtplib.SMTPServerDisconnected:
        init_email_api()
        smtp_api.sendmail(from_email, to_emails, email.as_string())  # 发送邮件


def close_email_api():
    global smtp_api
    if smtp_api is not None:
        smtp_api.quit()
