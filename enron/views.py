from django.shortcuts import render

# Create your views here.
from django.shortcuts import render
from django.db import connection
from django.http import Http404


def collaborator_list(request):
    q = (request.GET.get("q") or "").strip()

    sql = """
        SELECT
            c.id,
            c.employee_key,
            c.display_name,
            c.first_name,
            c.last_name,
            c.position_title,
            c.is_enron_employee,
            COUNT(ea.id) AS email_count,
            COALESCE(STRING_AGG(ea.email, ', ' ORDER BY ea.email), '') AS emails
        FROM enron_collaborator c
        LEFT JOIN enron_emailaddress ea
            ON ea.collaborator_id = c.id
        WHERE
            (
                %s = ''
                OR c.employee_key ILIKE %s
                OR c.display_name ILIKE %s
                OR c.first_name ILIKE %s
                OR c.last_name ILIKE %s
                OR ea.email ILIKE %s
            )
        GROUP BY
            c.id,
            c.employee_key,
            c.display_name,
            c.first_name,
            c.last_name,
            c.position_title,
            c.is_enron_employee
        ORDER BY
            c.display_name NULLS LAST,
            c.employee_key ASC
    """

    like_q = f"%{q}%"

    with connection.cursor() as cursor:
        cursor.execute(sql, [q, like_q, like_q, like_q, like_q, like_q])
        columns = [col[0] for col in cursor.description]
        collaborators = [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]

    return render(request, "collaborators/list.html", {
        "collaborators": collaborators,
        "q": q,
    })



def collaborator_detail(request, collaborator_id):
    collaborator_sql = """
        SELECT
            c.id,
            c.employee_key,
            c.display_name,
            c.first_name,
            c.last_name,
            c.position_title,
            c.is_enron_employee,
            c.notes
        FROM enron_collaborator c
        WHERE c.id = %s
    """

    emails_sql = """
        SELECT
            ea.id,
            ea.email,
            ea.local_part,
            ea.domain,
            ea.display_name
        FROM enron_emailaddress ea
        WHERE ea.collaborator_id = %s
        ORDER BY ea.email ASC
    """

    mailboxes_folders_sql = """
        SELECT
            m.id AS mailbox_id,
            m.mailbox_key,
            m.source_root_path,
            f.id AS folder_id,
            f.folder_name,
            f.folder_path,
            f.folder_type,
            f.folder_topic
        FROM enron_mailbox m
        LEFT JOIN enron_folder f
            ON f.mailbox_id = m.id
        WHERE m.owner_id = %s
        ORDER BY m.mailbox_key ASC, f.folder_path ASC
    """

    with connection.cursor() as cursor:
        cursor.execute(collaborator_sql, [collaborator_id])
        row = cursor.fetchone()
        if not row:
            raise Http404("Collaborateur introuvable")

        collaborator_columns = [col[0] for col in cursor.description]
        collaborator = dict(zip(collaborator_columns, row))

        cursor.execute(emails_sql, [collaborator_id])
        email_columns = [col[0] for col in cursor.description]
        emails = [
            dict(zip(email_columns, email_row))
            for email_row in cursor.fetchall()
        ]

        cursor.execute(mailboxes_folders_sql, [collaborator_id])
        mf_columns = [col[0] for col in cursor.description]
        mailboxes_folders_rows = [
            dict(zip(mf_columns, mf_row))
            for mf_row in cursor.fetchall()
        ]

    mailboxes_map = {}

    for row in mailboxes_folders_rows:
        mailbox_id = row["mailbox_id"]

        if mailbox_id not in mailboxes_map:
            mailboxes_map[mailbox_id] = {
                "id": row["mailbox_id"],
                "mailbox_key": row["mailbox_key"],
                "source_root_path": row["source_root_path"],
                "folders": [],
            }

        if row["folder_id"] is not None:
            mailboxes_map[mailbox_id]["folders"].append({
                "id": row["folder_id"],
                "folder_name": row["folder_name"],
                "folder_path": row["folder_path"],
                "folder_type": row["folder_type"],
                "folder_topic": row["folder_topic"],
            })

    mailboxes = list(mailboxes_map.values())
    total_folders = sum(len(mailbox["folders"]) for mailbox in mailboxes)

    return render(request, "collaborators/detail.html", {
        "collaborator": collaborator,
        "emails": emails,
        "mailboxes": mailboxes,
        "total_folders": total_folders,
    })


def folder_detail(request, folder_id):
    folder_sql = """
        SELECT
            f.id,
            f.folder_name,
            f.folder_path,
            f.folder_type,
            f.folder_topic,
            m.id AS mailbox_id,
            m.mailbox_key,
            m.source_root_path,
            c.id AS collaborator_id,
            c.employee_key,
            c.display_name
        FROM enron_folder f
        JOIN enron_mailbox m
            ON m.id = f.mailbox_id
        LEFT JOIN enron_collaborator c
            ON c.id = m.owner_id
        WHERE f.id = %s
    """

    messages_sql = """
        SELECT
            mo.id AS occurrence_id,
            mo.source_file,
            mo.validation_is_valid,
            mo.validation_errors,
            mo.validation_warnings,

            msg.id AS message_pk,
            msg.message_id,
            msg.sender_email,
            msg.sent_at,
            msg.subject_normalized,
            msg.has_attachments,
            msg.attachment_count,
            msg.parse_ok,
            msg.parse_error,
            msg.is_response,
            msg.is_forward,
            msg.references_depth,
            msg.quoted_line_count
        FROM enron_messageoccurrence mo
        JOIN enron_message msg
            ON msg.id = mo.message_id
        WHERE mo.folder_id = %s
        ORDER BY msg.sent_at DESC NULLS LAST, msg.id ASC
    """

    with connection.cursor() as cursor:
        cursor.execute(folder_sql, [folder_id])
        folder_row = cursor.fetchone()

        if not folder_row:
            raise Http404("Folder introuvable")

        folder_columns = [col[0] for col in cursor.description]
        folder = dict(zip(folder_columns, folder_row))

        cursor.execute(messages_sql, [folder_id])
        message_columns = [col[0] for col in cursor.description]
        messages = [
            dict(zip(message_columns, row))
            for row in cursor.fetchall()
        ]

    return render(request, "folders/detail.html", {
        "folder": folder,
        "messages": messages,
    })

def message_detail(request, occurrence_id):
    occurrence_sql = """
        SELECT
            mo.id AS occurrence_id,
            mo.source_file,
            mo.validation_is_valid,
            mo.validation_errors,
            mo.validation_warnings,

            f.id AS folder_id,
            f.folder_name,
            f.folder_path,
            f.folder_type,
            f.folder_topic,

            mb.id AS mailbox_id,
            mb.mailbox_key,
            mb.source_root_path,

            c.id AS collaborator_id,
            c.employee_key,
            c.display_name AS collaborator_display_name,

            msg.id AS message_pk,
            msg.message_id,
            msg.sender_email,
            msg.sent_at,
            msg.in_reply_to,
            msg.subject_normalized,
            msg.body_clean,
            msg.signature,
            msg.mime_type,
            msg.content_type_header,
            msg.has_attachments,
            msg.attachment_count,
            msg.parse_ok,
            msg.parse_error,
            msg.is_response,
            msg.is_forward,
            msg.response_to_message_id,
            msg.response_to_message_id_source,
            msg.thread_root_message_id,
            msg.references_depth,
            msg.quoted_line_count
        FROM enron_messageoccurrence mo
        JOIN enron_message msg
            ON msg.id = mo.message_id
        JOIN enron_folder f
            ON f.id = mo.folder_id
        JOIN enron_mailbox mb
            ON mb.id = mo.mailbox_id
        LEFT JOIN enron_collaborator c
            ON c.id = mb.owner_id
        WHERE mo.id = %s
    """

    recipients_sql = """
        SELECT
            mr.id,
            mr.recipient_type,
            mr.display_name,
            ea.email,
            ea.local_part,
            ea.domain
        FROM enron_messagerecipient mr
        JOIN enron_emailaddress ea
            ON ea.id = mr.email_address_id
        JOIN enron_messageoccurrence mo
            ON mo.message_id = mr.message_id
        WHERE mo.id = %s
        ORDER BY
            CASE mr.recipient_type
                WHEN 'to' THEN 1
                WHEN 'cc' THEN 2
                WHEN 'bcc' THEN 3
                ELSE 4
            END,
            ea.email ASC
    """

    attachments_sql = """
        SELECT
            a.id,
            a.filename,
            a.mime_type,
            a.content_id,
            a.size_bytes,
            a.storage_path,
            a.sha256
        FROM enron_attachment a
        JOIN enron_messageoccurrence mo
            ON mo.message_id = a.message_id
        WHERE mo.id = %s
        ORDER BY a.id ASC
    """

    references_sql = """
        SELECT
            mr.id,
            mr.referenced_message_id
        FROM enron_messagereference mr
        JOIN enron_messageoccurrence mo
            ON mo.message_id = mr.message_id
        WHERE mo.id = %s
        ORDER BY mr.id ASC
    """

    with connection.cursor() as cursor:
        cursor.execute(occurrence_sql, [occurrence_id])
        row = cursor.fetchone()
        if not row:
            raise Http404("Occurrence introuvable")

        occurrence_columns = [col[0] for col in cursor.description]
        occurrence = dict(zip(occurrence_columns, row))

        cursor.execute(recipients_sql, [occurrence_id])
        recipient_columns = [col[0] for col in cursor.description]
        recipients = [
            dict(zip(recipient_columns, r))
            for r in cursor.fetchall()
        ]

        cursor.execute(attachments_sql, [occurrence_id])
        attachment_columns = [col[0] for col in cursor.description]
        attachments = [
            dict(zip(attachment_columns, a))
            for a in cursor.fetchall()
        ]

        cursor.execute(references_sql, [occurrence_id])
        reference_columns = [col[0] for col in cursor.description]
        references = [
            dict(zip(reference_columns, ref))
            for ref in cursor.fetchall()
        ]

    recipients_by_type = {
        "to": [],
        "cc": [],
        "bcc": [],
    }
    for recipient in recipients:
        rtype = recipient["recipient_type"]
        if rtype in recipients_by_type:
            recipients_by_type[rtype].append(recipient)

    return render(request, "messages/detail.html", {
        "occurrence": occurrence,
        "recipients_by_type": recipients_by_type,
        "attachments": attachments,
        "references": references,
    })


def collaborator_sent_messages(request, collaborator_id):
    collaborator_sql = """
        SELECT
            c.id,
            c.employee_key,
            c.display_name,
            c.first_name,
            c.last_name,
            c.position_title,
            c.is_enron_employee
        FROM enron_collaborator c
        WHERE c.id = %s
    """

    aliases_sql = """
        SELECT
            ea.id,
            ea.email,
            ea.local_part,
            ea.domain,
            ea.display_name
        FROM enron_emailaddress ea
        WHERE ea.collaborator_id = %s
        ORDER BY ea.email ASC
    """

    sent_messages_sql = """
        SELECT DISTINCT
            msg.id,
            msg.message_id,
            msg.sender_email,
            msg.sent_at,
            msg.subject_normalized,
            msg.has_attachments,
            msg.attachment_count,
            msg.parse_ok,
            msg.is_response,
            msg.is_forward,
            msg.thread_root_message_id
        FROM enron_message msg
        LEFT JOIN enron_emailaddress se
            ON se.id = msg.sender_id
        WHERE
            se.collaborator_id = %s
            OR msg.sender_email IN (
                SELECT ea.email
                FROM enron_emailaddress ea
                WHERE ea.collaborator_id = %s
            )
        ORDER BY msg.sent_at DESC NULLS LAST, msg.id ASC
    """

    with connection.cursor() as cursor:
        cursor.execute(collaborator_sql, [collaborator_id])
        collaborator_row = cursor.fetchone()

        if not collaborator_row:
            raise Http404("Collaborateur introuvable")

        collaborator_columns = [col[0] for col in cursor.description]
        collaborator = dict(zip(collaborator_columns, collaborator_row))

        cursor.execute(aliases_sql, [collaborator_id])
        alias_columns = [col[0] for col in cursor.description]
        aliases = [
            dict(zip(alias_columns, row))
            for row in cursor.fetchall()
        ]

        cursor.execute(sent_messages_sql, [collaborator_id, collaborator_id])
        message_columns = [col[0] for col in cursor.description]
        messages = [
            dict(zip(message_columns, row))
            for row in cursor.fetchall()
        ]

    return render(request, "collaborators/sent_messages.html", {
        "collaborator": collaborator,
        "aliases": aliases,
        "messages": messages,
    })


def message_thread(request, message_id):
    current_message_sql = """
        SELECT
            m.id,
            m.message_id,
            m.thread_root_message_id,
            m.in_reply_to,
            m.response_to_message_id,
            m.subject_normalized,
            m.sender_email,
            m.sent_at
        FROM enron_message m
        WHERE m.id = %s
    """

    thread_messages_by_root_sql = """
        SELECT
            m.id,
            m.message_id,
            m.sender_email,
            m.sent_at,
            m.subject_normalized,
            m.in_reply_to,
            m.response_to_message_id,
            m.thread_root_message_id,
            m.is_response,
            m.is_forward,
            m.has_attachments,
            m.attachment_count,
            m.parse_ok
        FROM enron_message m
        WHERE
            m.thread_root_message_id = %s
            OR m.message_id = %s
        ORDER BY m.sent_at ASC NULLS LAST, m.id ASC
    """

    fallback_single_message_sql = """
        SELECT
            m.id,
            m.message_id,
            m.sender_email,
            m.sent_at,
            m.subject_normalized,
            m.in_reply_to,
            m.response_to_message_id,
            m.thread_root_message_id,
            m.is_response,
            m.is_forward,
            m.has_attachments,
            m.attachment_count,
            m.parse_ok
        FROM enron_message m
        WHERE m.id = %s
        ORDER BY m.sent_at ASC NULLS LAST, m.id ASC
    """

    with connection.cursor() as cursor:
        cursor.execute(current_message_sql, [message_id])
        current_row = cursor.fetchone()

        if not current_row:
            raise Http404("Message introuvable")

        current_columns = [col[0] for col in cursor.description]
        current_message = dict(zip(current_columns, current_row))

        thread_root = current_message["thread_root_message_id"]
        current_message_identifier = current_message["message_id"]

        if thread_root:
            cursor.execute(
                thread_messages_by_root_sql,
                [thread_root, thread_root],
            )
        else:
            cursor.execute(fallback_single_message_sql, [message_id])

        thread_columns = [col[0] for col in cursor.description]
        thread_messages = [
            dict(zip(thread_columns, row))
            for row in cursor.fetchall()
        ]

    return render(request, "messages/thread.html", {
        "current_message": current_message,
        "thread_messages": thread_messages,
    })



from django.shortcuts import render
from django.db import connection
from django.http import Http404
import json


def dashboard(request):
    summary_sql = """
        SELECT
            (SELECT COUNT(*) FROM enron_message) AS total_messages,
            (SELECT COUNT(*) FROM enron_collaborator) AS total_collaborators,
            (SELECT COUNT(*) FROM enron_mailbox) AS total_mailboxes,
            (SELECT COUNT(*) FROM enron_attachment) AS total_attachments
    """

    mails_per_month_sql = """
        SELECT
            DATE_TRUNC('month', sent_at) AS month,
            COUNT(*) AS message_count
        FROM enron_message
        WHERE sent_at IS NOT NULL
        GROUP BY DATE_TRUNC('month', sent_at)
        ORDER BY month ASC
    """

    top_senders_sql = """
        SELECT
            COALESCE(sender_email, '(unknown)') AS sender_email,
            COUNT(*) AS message_count
        FROM enron_message
        GROUP BY COALESCE(sender_email, '(unknown)')
        ORDER BY message_count DESC, sender_email ASC
        LIMIT 10
    """

    message_with_response_id_sql = """
        SELECT COUNT(*) AS message_with_response_count
        FROM enron_message
        WHERE response_to_message_id IS NOT NULL
    """

    with connection.cursor() as cursor:
        cursor.execute(summary_sql)
        summary_columns = [col[0] for col in cursor.description]
        summary = dict(zip(summary_columns, cursor.fetchone()))

        cursor.execute(mails_per_month_sql)
        month_columns = [col[0] for col in cursor.description]
        mails_per_month = [
            dict(zip(month_columns, row))
            for row in cursor.fetchall()
        ]

        cursor.execute(top_senders_sql)
        sender_columns = [col[0] for col in cursor.description]
        top_senders = [
            dict(zip(sender_columns, row))
            for row in cursor.fetchall()
        ]

        cursor.execute(message_with_response_id_sql)
        message_with_response_count = cursor.fetchone()[0]

    mails_per_month_labels = [
        row["month"].strftime("%Y-%m") if row["month"] else ""
        for row in mails_per_month
    ]
    mails_per_month_values = [
        row["message_count"]
        for row in mails_per_month
    ]

    top_senders_labels = [
        row["sender_email"]
        for row in top_senders
    ]
    top_senders_values = [
        row["message_count"]
        for row in top_senders
    ]

    return render(request, "dashboard.html", {
        "summary": summary,
        "mails_per_month": mails_per_month,
        "top_senders": top_senders,
        "message_with_response": message_with_response_count,
        "mails_per_month_labels_json": json.dumps(mails_per_month_labels),
        "mails_per_month_values_json": json.dumps(mails_per_month_values),
        "top_senders_labels_json": json.dumps(top_senders_labels),
        "top_senders_values_json": json.dumps(top_senders_values),
    })