def test_task(waiting_slot):
    import time
    print("Start : %s" % time.ctime())
    time.sleep(waiting_slot)
    print("End : %s" % time.ctime())
    # print("k : ", k)
    return waiting_slot
