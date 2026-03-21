from django.urls import path
from .views import collaborator_list, collaborator_detail, folder_detail, message_detail, collaborator_sent_messages, message_thread, dashboard, message_list

urlpatterns = [
    path("", dashboard, name="dashboard"),
    path("collaborators/", collaborator_list, name="collaborator_list"),
    path("collaborators/<int:collaborator_id>/", collaborator_detail, name="collaborator_detail"),
    path("folders/<int:folder_id>/", folder_detail, name="folder_detail"),
    path("messages/<int:message_id>/", message_detail, name="message_detail"),
    path("collaborators/<int:collaborator_id>/sent-messages/", collaborator_sent_messages, name="collaborator_sent_messages",),
    path("threads/<int:message_id>/", message_thread, name="message_thread"),
    path("messages/", message_list, name="message_list"),
]