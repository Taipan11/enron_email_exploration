from django.shortcuts import render, get_object_or_404
from django.db import connection
from django.http import Http404
from django.db.models import Q, Value, TextField, FloatField, CharField, Prefetch, F
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank, SearchHeadline
from django.db.models import Count, Q, Avg, Max
from django.db.models.functions import TruncMonth, ExtractHour, ExtractWeekDay, Coalesce, Left

from .models import (
    EmailAddress,
    Message,
    Collaborator,
    Mailbox,
    Attachment,
    Folder,
    MessageOccurrence,
)
from django.db.models import Count, Q, Value
from django.db.models.functions import Coalesce
from django.contrib.postgres.aggregates import StringAgg
from django.shortcuts import render


def collaborator_list(request):
    q = (request.GET.get("q") or "").strip()

    collaborators_qs = (
        Collaborator.objects
        .annotate(
            email_count=Count("email_addresses", distinct=True),
            emails=Coalesce(
                StringAgg(
                    "email_addresses__email",
                    delimiter=", ",
                    ordering="email_addresses__email",
                    distinct=True,
                    output_field=TextField(),
                ),
                Value("", output_field=TextField()),
                output_field=TextField(),
            ),
        )
    )

    if q:
        collaborators_qs = collaborators_qs.filter(
            Q(employee_key__icontains=q)
            | Q(display_name__icontains=q)
            | Q(first_name__icontains=q)
            | Q(last_name__icontains=q)
            | Q(email_addresses__email__icontains=q)
        ).distinct()

    collaborators = list(
        collaborators_qs
        .values(
            "id",
            "employee_key",
            "display_name",
            "first_name",
            "last_name",
            "position_title",
            "is_enron_employee",
            "email_count",
            "emails",
        )
        .order_by("display_name", "employee_key")
    )

    return render(request, "collaborators/list.html", {
        "collaborators": collaborators,
        "q": q,
    })

def collaborator_detail(request, collaborator_id):
    collaborator_obj = get_object_or_404(
        Collaborator.objects.all(),
        pk=collaborator_id,
    )

    emails_qs = (
        EmailAddress.objects
        .filter(collaborator_id=collaborator_id)
        .order_by("email")
    )

    mailboxes_qs = (
        Mailbox.objects
        .filter(owner_id=collaborator_id)
        .prefetch_related(
            Prefetch(
                "folders",
                queryset=Folder.objects.order_by("folder_path"),
            )
        )
        .order_by("mailbox_key")
    )

    collaborator = {
        "id": collaborator_obj.id,
        "employee_key": collaborator_obj.employee_key,
        "display_name": collaborator_obj.display_name,
        "first_name": collaborator_obj.first_name,
        "last_name": collaborator_obj.last_name,
        "position_title": collaborator_obj.position_title,
        "is_enron_employee": collaborator_obj.is_enron_employee,
        "notes": collaborator_obj.notes,
    }

    emails = [
        {
            "id": email.id,
            "email": email.email,
            "local_part": email.local_part,
            "domain": email.domain,
            "display_name": email.display_name,
        }
        for email in emails_qs
    ]

    mailboxes = [
        {
            "id": mailbox.id,
            "mailbox_key": mailbox.mailbox_key,
            "source_root_path": mailbox.source_root_path,
            "folders": [
                {
                    "id": folder.id,
                    "folder_name": folder.folder_name,
                    "folder_path": folder.folder_path,
                    "folder_type": folder.folder_type,
                    "folder_topic": folder.folder_topic,
                }
                for folder in mailbox.folders.all()
            ],
        }
        for mailbox in mailboxes_qs
    ]

    return render(request, "collaborators/detail.html", {
        "collaborator": collaborator,
        "emails": emails,
        "mailboxes": mailboxes,
    })


def folder_detail(request, folder_id):
    folder_obj = get_object_or_404(
        Folder.objects.select_related(
            "mailbox",
            "mailbox__owner",
        ),
        pk=folder_id,
    )

    mailbox = folder_obj.mailbox
    collaborator = getattr(mailbox, "owner", None)

    occurrences_qs = (
        MessageOccurrence.objects
        .filter(folder_id=folder_id)
        .select_related("message")
        .order_by(F("message__sent_at").desc(nulls_last=True), "message__id")
    )

    folder = {
        "id": folder_obj.id,
        "folder_name": folder_obj.folder_name,
        "folder_path": folder_obj.folder_path,
        "folder_type": folder_obj.folder_type,
        "folder_topic": folder_obj.folder_topic,
        "mailbox_id": mailbox.id if mailbox else None,
        "mailbox_key": mailbox.mailbox_key if mailbox else None,
        "source_root_path": mailbox.source_root_path if mailbox else None,
        "collaborator_id": collaborator.id if collaborator else None,
        "employee_key": collaborator.employee_key if collaborator else None,
        "display_name": collaborator.display_name if collaborator else None,
    }

    messages = [
        {
            "occurrence_id": occurrence.id,
            "source_file": occurrence.source_file,
            "validation_is_valid": occurrence.validation_is_valid,
            "validation_errors": occurrence.validation_errors,
            "validation_warnings": occurrence.validation_warnings,

            "message_pk": occurrence.message.id if occurrence.message else None,
            "message_id": occurrence.message.message_id if occurrence.message else None,
            "sender_email": occurrence.message.sender_email if occurrence.message else None,
            "sent_at": occurrence.message.sent_at if occurrence.message else None,
            "subject_normalized": occurrence.message.subject_normalized if occurrence.message else None,
            "has_attachments": occurrence.message.has_attachments if occurrence.message else False,
            "attachment_count": occurrence.message.attachment_count if occurrence.message else 0,
            "parse_ok": occurrence.message.parse_ok if occurrence.message else False,
            "parse_error": occurrence.message.parse_error if occurrence.message else None,
            "is_response": occurrence.message.is_response if occurrence.message else False,
            "is_forward": occurrence.message.is_forward if occurrence.message else False,
            "references_depth": occurrence.message.references_depth if occurrence.message else 0,
            "quoted_line_count": occurrence.message.quoted_line_count if occurrence.message else 0,
        }
        for occurrence in occurrences_qs
    ]

    return render(request, "folders/detail.html", {
        "folder": folder,
        "messages": messages,
    })

def message_detail(request, message_id):
    message = get_object_or_404(
        Message.objects.select_related("sender"),
        pk=message_id,
    )

    recipients_qs = (
        message.recipients
        .select_related("email_address")
        .order_by("recipient_type", "email_address__email", "id")
    )

    attachments_qs = message.attachments.order_by("id")
    references_qs = message.outgoing_references.order_by("id")

    occurrences_qs = (
        MessageOccurrence.objects
        .filter(message=message)
        .select_related("folder", "mailbox", "mailbox__owner")
        .order_by("id")
    )

    recipients = [
        {
            "id": recipient.id,
            "recipient_type": recipient.recipient_type,
            "display_name": recipient.display_name,
            "email": recipient.email_address.email if recipient.email_address else None,
            "local_part": recipient.email_address.local_part if recipient.email_address else None,
            "domain": recipient.email_address.domain if recipient.email_address else None,
        }
        for recipient in recipients_qs
    ]

    attachments = [
        {
            "id": attachment.id,
            "filename": attachment.filename,
            "mime_type": attachment.mime_type,
            "content_id": attachment.content_id,
            "size_bytes": attachment.size_bytes,
            "storage_path": attachment.storage_path,
            "sha256": attachment.sha256,
        }
        for attachment in attachments_qs
    ]

    references = [
        {
            "id": reference.id,
            "referenced_message_id": reference.referenced_message_id,
        }
        for reference in references_qs
    ]

    recipients_by_type = {
        "to": [],
        "cc": [],
        "bcc": [],
    }
    for recipient in recipients:
        rtype = (recipient["recipient_type"] or "").lower()
        if rtype in recipients_by_type:
            recipients_by_type[rtype].append(recipient)

    occurrences = [
        {
            "id": occ.id,
            "source_file": occ.source_file,
            "validation_is_valid": occ.validation_is_valid,
            "folder_id": occ.folder.id if occ.folder else None,
            "folder_name": occ.folder.folder_name if occ.folder else None,
            "folder_path": occ.folder.folder_path if occ.folder else None,
            "mailbox_key": occ.mailbox.mailbox_key if occ.mailbox else None,
            "collaborator_display_name": occ.mailbox.owner.display_name if occ.mailbox and occ.mailbox.owner else None,
        }
        for occ in occurrences_qs
    ]

    message_data = {
        "id": message.id,
        "message_id": message.message_id,
        "sender_email": message.sender_email,
        "sent_at": message.sent_at,
        "in_reply_to": message.in_reply_to,
        "subject_normalized": message.subject_normalized,
        "body_clean": message.body_clean,
        "body_html_clean": getattr(message, "body_html_clean", None),
        "signature": message.signature,
        "mime_type": message.mime_type,
        "content_type_header": message.content_type_header,
        "has_attachments": message.has_attachments,
        "attachment_count": message.attachment_count,
        "parse_ok": message.parse_ok,
        "parse_error": message.parse_error,
        "is_response": message.is_response,
        "is_forward": message.is_forward,
        "response_to_message_id": message.response_to_message_id,
        "response_to_message_id_source": message.response_to_message_id_source,
        "thread_root_message_id": message.thread_root_message_id,
        "references_depth": message.references_depth,
        "quoted_line_count": message.quoted_line_count,
        "canonical_hash": getattr(message, "canonical_hash", None),
    }

    return render(request, "messages/detail.html", {
        "message": message_data,
        "message_obj": message,
        "recipients_by_type": recipients_by_type,
        "attachments": attachments,
        "references": references,
        "occurrences": occurrences,
    })

def collaborator_sent_messages(request, collaborator_id):
    collaborator_obj = get_object_or_404(
        Collaborator.objects.all(),
        pk=collaborator_id,
    )

    aliases_qs = (
        EmailAddress.objects
        .filter(collaborator_id=collaborator_id)
        .order_by("email")
    )

    alias_emails = list(
        aliases_qs.values_list("email", flat=True)
    )

    messages_qs = (
        Message.objects
        .filter(
            Q(sender__collaborator_id=collaborator_id)
            | Q(sender_email__in=alias_emails)
        )
        .distinct()
        .order_by(F("sent_at").desc(nulls_last=True), "id")
    )

    collaborator = {
        "id": collaborator_obj.id,
        "employee_key": collaborator_obj.employee_key,
        "display_name": collaborator_obj.display_name,
        "first_name": collaborator_obj.first_name,
        "last_name": collaborator_obj.last_name,
        "position_title": collaborator_obj.position_title,
        "is_enron_employee": collaborator_obj.is_enron_employee,
    }

    aliases = [
        {
            "id": alias.id,
            "email": alias.email,
            "local_part": alias.local_part,
            "domain": alias.domain,
            "display_name": alias.display_name,
        }
        for alias in aliases_qs
    ]

    messages = [
        {
            "id": message.id,
            "message_id": message.message_id,
            "sender_email": message.sender_email,
            "sent_at": message.sent_at,
            "subject_normalized": message.subject_normalized,
            "has_attachments": message.has_attachments,
            "attachment_count": message.attachment_count,
            "parse_ok": message.parse_ok,
            "is_response": message.is_response,
            "is_forward": message.is_forward,
            "thread_root_message_id": message.thread_root_message_id,
        }
        for message in messages_qs
    ]

    return render(request, "collaborators/sent_messages.html", {
        "collaborator": collaborator,
        "aliases": aliases,
        "messages": messages,
    })


def message_thread(request, message_id):
    current_message_obj = get_object_or_404(
        Message.objects.all(),
        pk=message_id,
    )

    thread_root = current_message_obj.thread_root_message_id

    if thread_root:
        thread_messages_qs = (
            Message.objects
            .filter(
                Q(thread_root_message_id=thread_root)
                | Q(message_id=thread_root)
            )
            .order_by(F("sent_at").asc(nulls_last=True), "id")
        )
    else:
        thread_messages_qs = (
            Message.objects
            .filter(pk=message_id)
            .order_by(F("sent_at").asc(nulls_last=True), "id")
        )

    current_message = {
        "id": current_message_obj.id,
        "message_id": current_message_obj.message_id,
        "thread_root_message_id": current_message_obj.thread_root_message_id,
        "in_reply_to": current_message_obj.in_reply_to,
        "response_to_message_id": current_message_obj.response_to_message_id,
        "subject_normalized": current_message_obj.subject_normalized,
        "sender_email": current_message_obj.sender_email,
        "sent_at": current_message_obj.sent_at,
    }

    thread_messages = [
        {
            "id": message.id,
            "message_id": message.message_id,
            "sender_email": message.sender_email,
            "sent_at": message.sent_at,
            "subject_normalized": message.subject_normalized,
            "body_clean": message.body_clean,
            "in_reply_to": message.in_reply_to,
            "response_to_message_id": message.response_to_message_id,
            "thread_root_message_id": message.thread_root_message_id,
            "is_response": message.is_response,
            "is_forward": message.is_forward,
            "has_attachments": message.has_attachments,
            "attachment_count": message.attachment_count,
            "parse_ok": message.parse_ok,
        }
        for message in thread_messages_qs
    ]

    return render(request, "messages/thread.html", {
        "current_message": current_message,
        "thread_messages": thread_messages,
    })

def dashboard(request):
    total_messages = Message.objects.count()
    total_collaborators = Collaborator.objects.count()
    total_mailboxes = Mailbox.objects.count()
    total_attachments = Attachment.objects.count()

    message_with_response_count = Message.objects.exclude(
        response_to_message_id__isnull=True
    ).exclude(
        response_to_message_id=""
    ).count()

    parse_ok_count = Message.objects.filter(parse_ok=True).count()
    with_attachments_count = Message.objects.filter(has_attachments=True).count()
    response_count = Message.objects.filter(is_response=True).count()
    forward_count = Message.objects.filter(is_forward=True).count()

    summary = {
        "total_messages": total_messages,
        "total_collaborators": total_collaborators,
        "total_mailboxes": total_mailboxes,
        "total_attachments": total_attachments,
        "message_with_response": message_with_response_count,
        "parse_ok_rate": round((parse_ok_count / total_messages) * 100, 2) if total_messages else 0,
        "with_attachments_rate": round((with_attachments_count / total_messages) * 100, 2) if total_messages else 0,
        "response_rate": round((response_count / total_messages) * 100, 2) if total_messages else 0,
        "forward_rate": round((forward_count / total_messages) * 100, 2) if total_messages else 0,
    }

    mails_per_month = list(
        Message.objects.filter(sent_at__isnull=False)
        .annotate(month=TruncMonth("sent_at"))
        .values("month")
        .annotate(
            message_count=Count("id"),
            response_count=Count("id", filter=Q(is_response=True)),
            forward_count=Count("id", filter=Q(is_forward=True)),
            attachment_count=Count("id", filter=Q(has_attachments=True)),
        )
        .order_by("month")
    )

    top_senders = list(
    Message.objects.annotate(
        sender_label=Coalesce(
            "sender_email",
            Value("(unknown)", output_field=CharField()),
        )
    )
    .values("sender_label")
    .annotate(message_count=Count("id"))
    .order_by("-message_count", "sender_label")[:10]
)

    top_domains = list(
        Message.objects.filter(sender_email__isnull=False)
        .exclude(sender_email="")
        .values("sender__domain")
        .annotate(message_count=Count("id"))
        .order_by("-message_count", "sender__domain")[:10]
    )

    top_mailboxes = list(
        MessageOccurrence.objects.values("mailbox__mailbox_key")
        .annotate(message_count=Count("id"))
        .order_by("-message_count", "mailbox__mailbox_key")[:10]
    )

    top_folders = list(
        MessageOccurrence.objects.values("folder__folder_path")
        .annotate(message_count=Count("id"))
        .order_by("-message_count", "folder__folder_path")[:10]
    )

    hourly_distribution = list(
        Message.objects.filter(sent_at__isnull=False)
        .annotate(hour=ExtractHour("sent_at"))
        .values("hour")
        .annotate(message_count=Count("id"))
        .order_by("hour")
    )

    weekday_distribution = list(
        Message.objects.filter(sent_at__isnull=False)
        .annotate(weekday=ExtractWeekDay("sent_at"))
        .values("weekday")
        .annotate(message_count=Count("id"))
        .order_by("weekday")
    )

    attachment_distribution = list(
        Message.objects.values("attachment_count")
        .annotate(message_count=Count("id"))
        .order_by("attachment_count")[:15]
    )

    depth_distribution = list(
        Message.objects.values("references_depth")
        .annotate(message_count=Count("id"))
        .order_by("references_depth")[:15]
    )

    quoted_distribution = list(
        Message.objects.values("quoted_line_count")
        .annotate(message_count=Count("id"))
        .order_by("quoted_line_count")[:20]
    )

    quality_stats = {
        "missing_sent_at": Message.objects.filter(sent_at__isnull=True).count(),
        "missing_sender_email": Message.objects.filter(
            Q(sender_email__isnull=True) | Q(sender_email="")
        ).count(),
        "missing_subject": Message.objects.filter(
            Q(subject_normalized__isnull=True) | Q(subject_normalized="")
        ).count(),
        "missing_message_id": Message.objects.filter(
            Q(message_id__isnull=True) | Q(message_id="")
        ).count(),
        "parse_error_count": Message.objects.filter(parse_ok=False).count(),
    }

    context = {
        "summary": summary,
        "mails_per_month": mails_per_month,
        "top_senders": top_senders,
        "top_domains": top_domains,
        "top_mailboxes": top_mailboxes,
        "top_folders": top_folders,
        "hourly_distribution": hourly_distribution,
        "weekday_distribution": weekday_distribution,
        "attachment_distribution": attachment_distribution,
        "depth_distribution": depth_distribution,
        "quoted_distribution": quoted_distribution,
        "quality_stats": quality_stats,
    }
    return render(request, "dashboard.html", context)


def message_list(request):
    q = (request.GET.get("q") or "").strip()
    sender_email = (request.GET.get("sender_email") or "").strip()
    subject = (request.GET.get("subject") or "").strip()
    thread_root_message_id = (request.GET.get("thread_root_message_id") or "").strip()
    message_id = (request.GET.get("message_id") or "").strip()
    in_reply_to = (request.GET.get("in_reply_to") or "").strip()

    sent_at_from = (request.GET.get("sent_at_from") or "").strip()
    sent_at_to = (request.GET.get("sent_at_to") or "").strip()

    has_attachments = (request.GET.get("has_attachments") or "").strip()
    has_html = (request.GET.get("has_html") or "").strip()
    parse_ok = (request.GET.get("parse_ok") or "").strip()
    is_response = (request.GET.get("is_response") or "").strip()
    is_forward = (request.GET.get("is_forward") or "").strip()

    attachment_count_min = (request.GET.get("attachment_count_min") or "").strip()
    attachment_count_max = (request.GET.get("attachment_count_max") or "").strip()
    references_depth_min = (request.GET.get("references_depth_min") or "").strip()
    references_depth_max = (request.GET.get("references_depth_max") or "").strip()
    quoted_line_count_min = (request.GET.get("quoted_line_count_min") or "").strip()
    quoted_line_count_max = (request.GET.get("quoted_line_count_max") or "").strip()

    ordering = (request.GET.get("ordering") or "").strip()

    allowed_orderings = {
        "-sent_at": ["-sent_at", "message_id"],
        "sent_at": ["sent_at", "message_id"],
        "-rank": ["-rank", "-sent_at", "message_id"],
        "sender_email": ["sender_email", "-sent_at", "message_id"],
        "-sender_email": ["-sender_email", "-sent_at", "message_id"],
        "subject_normalized": ["subject_normalized", "-sent_at", "message_id"],
        "-subject_normalized": ["-subject_normalized", "-sent_at", "message_id"],
        "attachment_count": ["attachment_count", "-sent_at", "message_id"],
        "-attachment_count": ["-attachment_count", "-sent_at", "message_id"],
        "references_depth": ["references_depth", "-sent_at", "message_id"],
        "-references_depth": ["-references_depth", "-sent_at", "message_id"],
        "quoted_line_count": ["quoted_line_count", "-sent_at", "message_id"],
        "-quoted_line_count": ["-quoted_line_count", "-sent_at", "message_id"],
    }

    qs = Message.objects.select_related("sender").all()

    # Important : reconstruire la même expression que celle utilisée dans ton GinIndex
    vector = SearchVector(
        Coalesce(
            "subject_normalized",
            Value("", output_field=TextField()),
            output_field=TextField(),
        ),
        Coalesce(
            Left("body_clean", 200000),
            Value("", output_field=TextField()),
            output_field=TextField(),
        ),
        config="english",
    )

    if q:
        query = SearchQuery(q, config="english", search_type="websearch")
        qs = qs.annotate(
            rank=SearchRank(vector, query),
            headline_subject=SearchHeadline(
                "subject_normalized",
                query,
                config="english",
                start_sel="<mark><strong>",
                stop_sel="</strong></mark>",
                max_words=20,
                min_words=5,
                short_word=2,
                highlight_all=True,
                max_fragments=1,
            ),
            headline_body=SearchHeadline(
                "body_clean",
                query,
                config="english",
                start_sel="<mark><strong>",
                stop_sel="</strong></mark>",
                max_words=35,
                min_words=15,
                short_word=2,
                highlight_all=True,
                max_fragments=3,
                fragment_delimiter=" … ",
            ),
        ).filter(rank__gt=0)
    else:
        qs = qs.annotate(rank=Value(None, output_field=FloatField()))

    if sender_email:
        qs = qs.filter(sender_email__icontains=sender_email)

    if subject:
        qs = qs.filter(subject_normalized__icontains=subject)

    if thread_root_message_id:
        qs = qs.filter(thread_root_message_id__icontains=thread_root_message_id)

    if message_id:
        qs = qs.filter(message_id__icontains=message_id)

    if in_reply_to:
        qs = qs.filter(in_reply_to__icontains=in_reply_to)

    if sent_at_from:
        qs = qs.filter(sent_at__gte=sent_at_from)

    if sent_at_to:
        qs = qs.filter(sent_at__lte=sent_at_to)

    if has_attachments == "true":
        qs = qs.filter(has_attachments=True)
    elif has_attachments == "false":
        qs = qs.filter(has_attachments=False)

    if has_html == "true":
        qs = qs.exclude(body_html_clean__isnull=True).exclude(body_html_clean="")
    elif has_html == "false":
        qs = qs.filter(Q(body_html_clean__isnull=True) | Q(body_html_clean=""))

    if parse_ok == "true":
        qs = qs.filter(parse_ok=True)
    elif parse_ok == "false":
        qs = qs.filter(parse_ok=False)

    if is_response == "true":
        qs = qs.filter(is_response=True)
    elif is_response == "false":
        qs = qs.filter(is_response=False)

    if is_forward == "true":
        qs = qs.filter(is_forward=True)
    elif is_forward == "false":
        qs = qs.filter(is_forward=False)

    if attachment_count_min:
        try:
            qs = qs.filter(attachment_count__gte=int(attachment_count_min))
        except ValueError:
            pass

    if attachment_count_max:
        try:
            qs = qs.filter(attachment_count__lte=int(attachment_count_max))
        except ValueError:
            pass

    if references_depth_min:
        try:
            qs = qs.filter(references_depth__gte=int(references_depth_min))
        except ValueError:
            pass

    if references_depth_max:
        try:
            qs = qs.filter(references_depth__lte=int(references_depth_max))
        except ValueError:
            pass

    if quoted_line_count_min:
        try:
            qs = qs.filter(quoted_line_count__gte=int(quoted_line_count_min))
        except ValueError:
            pass

    if quoted_line_count_max:
        try:
            qs = qs.filter(quoted_line_count__lte=int(quoted_line_count_max))
        except ValueError:
            pass

    if not ordering:
        ordering = "-rank" if q else "-sent_at"

    if ordering not in allowed_orderings:
        ordering = "-rank" if q else "-sent_at"

    qs = qs.order_by(*allowed_orderings[ordering])

    messages = qs[:200]

    return render(request, "messages/list.html", {
        "messages": messages,
        "q": q,
        "sender_email": sender_email,
        "subject": subject,
        "thread_root_message_id": thread_root_message_id,
        "message_id": message_id,
        "in_reply_to": in_reply_to,
        "sent_at_from": sent_at_from,
        "sent_at_to": sent_at_to,
        "has_attachments": has_attachments,
        "has_html": has_html,
        "parse_ok": parse_ok,
        "is_response": is_response,
        "is_forward": is_forward,
        "attachment_count_min": attachment_count_min,
        "attachment_count_max": attachment_count_max,
        "references_depth_min": references_depth_min,
        "references_depth_max": references_depth_max,
        "quoted_line_count_min": quoted_line_count_min,
        "quoted_line_count_max": quoted_line_count_max,
        "ordering": ordering,
        "result_count": len(messages),
    })

