try:
    from debug_utils import LOG_CURRENT_EXCEPTION, LOG_WARNING, LOG_NOTE
except ImportError:
    import traceback

    def LOG_CURRENT_EXCEPTION():
        traceback.print_exc()

    def LOG_WARNING(msg):
        print msg

    def LOG_NOTE(msg):
        print msg