from python_settings import settings
import settings as local_settings

settings.configure(local_settings)
assert settings.configured
