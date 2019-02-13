from importlib import import_module


def run_alerts_service():
    sync_gsheet_and_create_wordpress_posts()
    process_active_campaign_messages()


def sync_gsheet_and_create_wordpress_posts():
    wp_processor = import_module('sheets-wordpress')
    try:
        wp_processor.create_wordpress_posts()
    except Exception as e:
        print("ERROR: sheets-wordpress.py failed: {}".format(e))


def process_active_campaign_messages():
    post_tracker = import_module('ac-wp-post-track')
    try:
        post_tracker.main()
    except Exception as e:
        print("ERROR: ac-wp-post-track.py failed: {}".format(e))

    ac_processor = import_module('ac-auto')

    try:
        ac_processor.main()
    except Exception as e:
        print("ERROR: ac-auto.py failed: {}".format(e))


if __name__ == '__main__':
    from datetime import datetime
    from multiprocessing import Process
    from filelock import FileLock, Timeout

    TIMEOUT_SECONDS = 60*10
    lock = FileLock('alerts_service.py.lock', timeout=15)

    try:

        with lock.acquire():

            p = Process(target=run_alerts_service)
            p.start()
            p.join(TIMEOUT_SECONDS)

            if p.is_alive():
                print("{}: ERROR: Force terminated after {} seconds".format(
                       datetime.now(), TIMEOUT_SECONDS))
                p.terminate()
                p.join()

    except Timeout:
        print("{}: timeout: alerts_service blocked by another running instance.".format(datetime.now()))
