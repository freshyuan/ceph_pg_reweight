import commands
import sys
import time


def get_pool_id(pool_name):
    result = {}
    cmd = "ceph df | awk 'NR>5{print $1,$2}'"
    (status, output) = commands.getstatusoutput(cmd)
    pool_list = output.split('\n')
    for pool in pool_list:
        pool_info = pool.split(' ')
        result[pool_info[0]] = pool_info[1]
    return int(result[pool_name])


def get_pg_info(pool_id):
    osd_info = {}
    cmd = "ceph pg dump|grep '^%s\.'|awk -F ' ' '{print $1, $15}'" %(pool_id)
    (status, output) = commands.getstatusoutput(cmd)
    pg_info_list = output.split('\n')
    for pg in pg_info_list:
        pg_info = pg.split(' ')
        osd_list = pg_info[1].strip('[').strip(']').split(',')
        for osd in osd_list:
            if osd in osd_info:
                osd_info[osd] += 1
            else:
                osd_info[osd] = 1
    osd_info.pop('all')
    osd_info = sorted(osd_info.items(), key=lambda item: item[1])
    return osd_info


def get_osd_weight(osd_num):
    cmd = "ceph osd tree|grep ' osd.%s '|awk '{print $2}'" % osd_num
    weight = commands.getoutput(cmd)
    return float(weight)


def reweight():
    pg_total = 0
    pool_name = sys.argv[1]
    pool_id = get_pool_id(pool_name)
    osd_info = get_pg_info(pool_id)
    for osd in osd_info:
        pg_total += int(osd[1])
    pg_max = pg_total / len(osd_info) + 5
    pg_min = pg_total / len(osd_info) - 5
    print "==========target pg_max is: %s, target pg_min is:%s==========" % (pg_max, pg_min)
    osd_max = osd_info[-1][0]
    osd_min = osd_info[0][0]
    while True:
        if osd_info[0][1] < pg_min:
            raw_weight = get_osd_weight(osd_min)
            differ = float(osd_info[-1][1]) - float(osd_info[0][1])
            factor = abs(differ*1.2/1000)
            cmd = "ceph osd crush reweight osd.%s %s" % (osd_min, raw_weight+factor)
            print "==========%s==========" % commands.getoutput(cmd)
            time.sleep(1)
            osd_info = get_pg_info(pool_id)
            osd_min = osd_info[0][0]
            print "==========the current pg_min is %s==========" % str(osd_info[0])
            print "==========the current increase factor is: %s==========" % (differ*1.2/1000)
        if osd_info[-1][1] > pg_max:
            raw_weight = get_osd_weight(osd_max)
            differ = float(osd_info[-1][1]) - float(osd_info[0][1])
            factor = abs(differ*1.2/1000)
            cmd = "ceph osd crush reweight osd.%s %s" % (osd_max, raw_weight-factor)
            print "==========%s==========" % commands.getoutput(cmd)
            time.sleep(1)
            osd_info = get_pg_info(pool_id)
            osd_max = osd_info[-1][0]
            print "==========the current pg_max is %s==========" % str(osd_info[-1])
            print "==========the current decrease factor is: %s==========" % (differ*1.2/1000)
        else:
            print "==========the current pg_min is %s==========" % str(osd_info[0])
            print "==========the current pg_max is %s==========" % str(osd_info[-1])
            break

if __name__ == '__main__':
    reweight()
