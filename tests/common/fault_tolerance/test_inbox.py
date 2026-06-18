from common.fault_tolerance.inbox.inbox import Inbox
from common.fault_tolerance.inbox.inbox_status import InboxStatus

CLIENT = 123
SENDER = 0


def test_unseen_message_is_new():
    inbox = Inbox()
    assert inbox.classify(CLIENT, SENDER, 1) is InboxStatus.NEW


def test_applied_message_is_classified_applied():
    inbox = Inbox()
    inbox.mark_applied(CLIENT, SENDER, 1)
    assert inbox.classify(CLIENT, SENDER, 1) is InboxStatus.APPLIED


def test_done_message_is_classified_done():
    inbox = Inbox()
    inbox.mark_applied(CLIENT, SENDER, 1)
    inbox.mark_done(CLIENT, SENDER, 1)
    assert inbox.classify(CLIENT, SENDER, 1) is InboxStatus.DONE


def test_mark_done_clears_applied():
    inbox = Inbox()
    inbox.mark_applied(CLIENT, SENDER, 1)
    inbox.mark_done(CLIENT, SENDER, 1)
    # not lingering as APPLIED
    assert inbox.classify(CLIENT, SENDER, 1) is InboxStatus.DONE


def test_senders_are_tracked_independently():
    inbox = Inbox()
    inbox.mark_applied(CLIENT, 0, 5)
    inbox.mark_done(CLIENT, 0, 5)
    # same seq from a different sender is still new
    assert inbox.classify(CLIENT, 1, 5) is InboxStatus.NEW
    assert inbox.classify(CLIENT, 0, 5) is InboxStatus.DONE


def test_clients_are_tracked_independently():
    inbox = Inbox()
    inbox.mark_applied(1, SENDER, 1)
    inbox.mark_done(1, SENDER, 1)
    assert inbox.classify(2, SENDER, 1) is InboxStatus.NEW


def test_out_of_order_gap_is_new_until_filled():
    inbox = Inbox()
    for seq in (1, 2, 5):
        inbox.mark_applied(CLIENT, SENDER, seq)
        inbox.mark_done(CLIENT, SENDER, seq)
    assert inbox.classify(CLIENT, SENDER, 3) is InboxStatus.NEW   # gap
    assert inbox.classify(CLIENT, SENDER, 2) is InboxStatus.DONE  # filled


def test_drop_client_forgets_everything():
    inbox = Inbox()
    inbox.mark_applied(CLIENT, SENDER, 1)
    inbox.mark_done(CLIENT, SENDER, 1)
    inbox.drop_client(CLIENT)
    assert inbox.classify(CLIENT, SENDER, 1) is InboxStatus.NEW


def test_round_trip_empty():
    restored = Inbox.deserialize(Inbox().serialize())
    assert restored.classify(CLIENT, SENDER, 1) is InboxStatus.NEW


def test_round_trip_preserves_classification():
    inbox = Inbox()
    inbox.mark_applied(CLIENT, 0, 1)
    inbox.mark_done(CLIENT, 0, 1)
    inbox.mark_applied(CLIENT, 0, 5)      # done with a gap at 2..4
    inbox.mark_done(CLIENT, 0, 5)
    inbox.mark_applied(CLIENT, 1, 9)      # still applied, not done

    restored = Inbox.deserialize(inbox.serialize())

    assert restored.classify(CLIENT, 0, 1) is InboxStatus.DONE
    assert restored.classify(CLIENT, 0, 3) is InboxStatus.NEW
    assert restored.classify(CLIENT, 0, 5) is InboxStatus.DONE
    assert restored.classify(CLIENT, 1, 9) is InboxStatus.APPLIED
