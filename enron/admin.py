# Register your models here.
from django.contrib import admin

from .models import (
    Attachment,
    Collaborator,
    EmailAddress,
    Message,
    MessageRecipient,
    MessageReference,
    Folder,
    Mailbox,
)

admin.site.register(Collaborator)
admin.site.register(Mailbox)
admin.site.register(Folder)
admin.site.register(EmailAddress)
admin.site.register(MessageReference)
admin.site.register(Message)
admin.site.register(MessageRecipient)
admin.site.register(Attachment)
