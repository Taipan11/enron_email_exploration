from django.db import models

from .base import TimeStampedModel


class Collaborator(TimeStampedModel):
    employee_key = models.CharField(max_length=255, unique=True, db_index=True)
    display_name = models.CharField(max_length=255, blank=True, null=True)
    first_name = models.CharField(max_length=120, blank=True, null=True)
    last_name = models.CharField(max_length=120, blank=True, null=True)
    position_title = models.CharField(max_length=120, blank=True, null=True)
    is_enron_employee = models.BooleanField(default=True)
    notes = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ["employee_key"]

    def __str__(self) -> str:
        return self.display_name or self.employee_key