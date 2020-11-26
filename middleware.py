import fnmatch

from django.conf import settings
from django.http import HttpResponseForbidden

from whitenoise.middleware import WhiteNoiseMiddleware


class AuthenticatedWhiteNoiseMiddleware(WhiteNoiseMiddleware):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.auth_paths = getattr(settings, 'WHITENOISE_AUTHENTICATED_PATHS', None) or []
        self.auth_cookie = getattr(settings, 'WHITENOISE_AUTH_COOKIE', None)
        self.auth_cookie_domain = getattr(settings, 'WHITENOISE_AUTH_COOKIE_DOMAIN', None)
        self.auth_cookie_secure = getattr(settings, 'WHITENOISE_AUTH_COOKIE_SECURE', None)

    def __call__(self, request):
        response = super().__call__(request)
        response = self.process_response(request, response)
        return response

    def process_response(self, request, response):
        if self.auth_cookie and hasattr(request, "user") and request.user.is_staff:
            # User is authorized: add the auth cookie.
            response.set_signed_cookie(self.auth_cookie, "1",
                                       domain=self.auth_cookie_domain,
                                       secure=self.auth_cookie_secure,
                                       httponly=True)
        return response

    def serve(self, static_file, request):
        if self.auth_cookie:
            # Configured to enable authentication, let's do it!
            path = request.path_info
            auth_needed = any(fnmatch.fnmatch(path, pattern) for pattern in self.auth_paths)
            if auth_needed and not request.get_signed_cookie(self.auth_cookie, default=False):
                # Not authenticated even if needed: too bad…
                return HttpResponseForbidden()

        return super().serve(static_file, request)
