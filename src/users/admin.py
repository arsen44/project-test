from django.contrib import admin
from users.models import User
from .models import EventOutbox


@admin.register(User)
class UserAdmin(admin.ModelAdmin):
    pass


@admin.register(EventOutbox)
class EventOutboxAdmin(admin.ModelAdmin):
    list_display = ('event_type', 'event_date_time',
                    'environment', 'is_processed')
    list_filter = ('environment', 'is_processed')
    search_fields = ('event_type', 'event_context')
