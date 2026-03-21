from django import forms

SORT_CHOICES = [
    ("-sent_at", "Date décroissante"),
    ("sent_at", "Date croissante"),
    ("-rank", "Pertinence"),
    ("sender_email", "Expéditeur A-Z"),
    ("-sender_email", "Expéditeur Z-A"),
    ("subject_normalized", "Sujet A-Z"),
    ("-subject_normalized", "Sujet Z-A"),
]

YES_NO_ANY = [
    ("", "Tous"),
    ("true", "Oui"),
    ("false", "Non"),
]


class MessageSearchForm(forms.Form):
    q = forms.CharField(
        required=False,
        label="Recherche plein texte",
        widget=forms.TextInput(attrs={
            "class": "form-control",
            "placeholder": "Sujet, contenu..."
        }),
    )

    sent_at_from = forms.DateTimeField(
        required=False,
        input_formats=["%Y-%m-%dT%H:%M", "%Y-%m-%d", "%Y-%m-%d %H:%M"],
        widget=forms.DateTimeInput(attrs={"class": "form-control", "type": "datetime-local"}),
        label="Date min",
    )
    sent_at_to = forms.DateTimeField(
        required=False,
        input_formats=["%Y-%m-%dT%H:%M", "%Y-%m-%d", "%Y-%m-%d %H:%M"],
        widget=forms.DateTimeInput(attrs={"class": "form-control", "type": "datetime-local"}),
        label="Date max",
    )

    sender_email = forms.CharField(
        required=False,
        label="Expéditeur",
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )

    has_attachments = forms.ChoiceField(
        required=False,
        choices=YES_NO_ANY,
        label="Pièces jointes",
        widget=forms.Select(attrs={"class": "form-select"}),
    )

    is_response = forms.ChoiceField(
        required=False,
        choices=YES_NO_ANY,
        label="Réponse",
        widget=forms.Select(attrs={"class": "form-select"}),
    )

    is_forward = forms.ChoiceField(
        required=False,
        choices=YES_NO_ANY,
        label="Transfert",
        widget=forms.Select(attrs={"class": "form-select"}),
    )

    parse_ok = forms.ChoiceField(
        required=False,
        choices=YES_NO_ANY,
        label="Parse OK",
        widget=forms.Select(attrs={"class": "form-select"}),
    )

    thread_root_message_id = forms.CharField(
        required=False,
        label="Thread root",
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )

    ordering = forms.ChoiceField(
        required=False,
        choices=SORT_CHOICES,
        initial="-sent_at",
        label="Tri",
        widget=forms.Select(attrs={"class": "form-select"}),
    )