#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import csv
import time
import logging
import datetime as dt
from unicodedata import category
import MySQLdb
import DBConnections
import requests

logPath = "/var/www/html/vistara_db_scripts/logs/customUpdateDataUtilizationReport.log"
logging.basicConfig(
    filename=logPath, level=logging.DEBUG, format="%(asctime)s %(message)s"
)
logging.debug(
    """

 ------------------- Script started at:%s -----------------"""
    % dt.datetime.utcnow()
)
print("Script started at:%s" % dt.datetime.utcnow())

# db = MySQLdb.connect(host = 'reports_writw_db' , port = 3306, user = 'kayako_tickets', passwd = 'net3nr1ch', db = 'kayako_csd')


def dictfetchall(cursor):
    """Returns all rows from a cursor as a dict"""

    desc = cursor.description
    return [dict(zip([col[0] for col in desc], row)) for row in cursor.fetchall()]


# Fetch current and yesterday dates
def fetchCurrentAndYesterDayForUtilReportUpdateData():
    try:
        dbobj = DBConnections.DataBaseConn()
        db = dbobj.connetReadDB()
        cur = db.cursor()
        sql1 = "SELECT UNIX_TIMESTAMP(CONVERT_TZ(DATE_FORMAT(now(), '%Y-%m-%d 00:00:00'),'US/Pacific','UTC')) as currentTimeInPST,UNIX_TIMESTAMP(CONVERT_TZ(DATE_FORMAT(now(), '%Y-%m-%d 00:00:00') - interval 1 day,'US/Pacific','UTC')) as yesterDayPST  FROM dual"
        cur.execute(sql1)
        result1 = dictfetchall(cur)
        logging.debug(
            "methodName::fetchCurrentAndYesterDayForUtilReportUpdateData :"
            + str(result1)
        )
        db.close()
        cur.close()
        return result1
    except Exception as e:
        logging.debug(
            "Exception in fetchCurrentAndYesterDayForUtilReportUpdateData method:: =>"
            + str(e)
        )


def ownersStaffid():
    try:
        dbobj = DBConnections.DataBaseConn()
        db = dbobj.connetReadDB()
        cur = db.cursor()
        sql1 = "SELECT staffid, fullname FROM swstaff  WHERE staffgroupid != 41 ORDER BY fullname"
        cur.execute(sql1)
        result = dictfetchall(cur)
        SwaffIdSet = {}
        for i in result:
            SwaffIdSet[i["staffid"]] = i["fullname"]
        # print SwaffIdSet
        db.close()
        cur.close()
        return SwaffIdSet
    except Exception as e:
        logging.debug("Exception in ownersStaffid method:: =>" + str(e))


def getManagersInfo():
    try:
        dbobj = DBConnections.DataBaseConn()
        db = dbobj.connetRosterReadDB()
        cur = db.cursor()
        sql1 = "SELECT DISTINCT(staff.id), staff.swstaff_id, staff.staff_email, concat(staff.staff_fname,' ', staff.staff_lname) as staff_fullname FROM nr_staff staff INNER JOIN (SELECT DISTINCT(reporting_manager_id) AS mngr_id FROM nr_staff where is_active = 1 and is_found = 1 and swstaff_id > 0) nrs ON staff.id = nrs.mngr_id ORDER BY staff.staff_email"
        cur.execute(sql1)
        result1 = dictfetchall(cur)
        logging.debug("methodName::getManagersInfo ::" + str(result1))
        db.close()
        cur.close()
        return result1
    except Exception as e:
        logging.debug("Exception in getManagersInfo method:: =>" + str(e))


def getSwStaff():
    try:
        dbobj = DBConnections.DataBaseConn()
        db = dbobj.connetRosterReadDB()
        cur = db.cursor()
        sql1 = "SELECT DISTINCT(staff.id), staff.staff_fname, staff.staff_lname, concat(staff.staff_fname,' ', staff.staff_lname) as staff_fullname, staff.employee_id, addept.dept_name, rosterdept.dept_name AS roster, staff.user_name as samaccountname, staff.staff_email, staff.swstaff_id as staffid, staff.reporting_manager_id FROM nr_staff staff LEFT JOIN nr_departments addept ON (addept.id = staff.dept_id) LEFT JOIN nr_departments rosterdept ON (rosterdept.id = staff.roster_id) WHERE staff.is_active = 1 AND staff.is_found = 1 AND swstaff_id > 0 ORDER BY roster,staff.staff_fname, staff.staff_lname"
        cur.execute(sql1)
        result1 = dictfetchall(cur)
        logging.debug("methodName::getSwStaff, Fetching details :" + str(result1))
        db.close()
        cur.close()
        return result1
    except Exception as e:
        logging.debug("Exception in getSwStaff method:: =>" + str(e))


def fetchDetailsFromSwTicketPost(from_date, to_date):
    try:
        dbobj = DBConnections.DataBaseConn()
        db = dbobj.connetReadDB()
        cur = db.cursor()
        sql = "SELECT staffid, dateline FROM swticketposts WHERE dateline >= %s AND dateline <= %s AND creator = 1"
        cur.execute(sql, (from_date, to_date))
        sw_ticket_post = dictfetchall(cur)
        logging.debug(
            "methodName::fetchDetailsFromSwTicketPost, Fetch details from sw_ticket_post :"
            + str(sw_ticket_post)
        )
        db.close()
        cur.close()
        return sw_ticket_post
    except Exception as e:
        logging.debug(
            "Exception in fetchDetailsFromSwTicketPost method :: => " + str(e)
        )


def insertDetailsIntoStaffPostupdateTime(details):
    try:
        tableName = "staff_postupdate_time"
        for detail in details:
            if detail:
                staff_data = {}
                staff_data["staffid"] = int(detail["staffid"])
                staff_data["dateline"] = int(detail["dateline"])
                # sql = "insert into staff_postupdate_time(staffid,dateline) VALUES(%s,%s)"
                insertRecord(staff_data, tableName)
                logging.debug(
                    "methodName::insertDetailsIntoStaffPostupdateTime, Insert details into staff_postupdate_time table successfully"
                )
    except Exception as e:
        logging.debug(
            "Exception in insertDetailsIntoStaffPostupdateTime method:: => " + str(e)
        )


def getUserUtilizationDataDaywise(from_date, to_date):
    try:
        day_wise_data = []
        staff_data = ownersStaffid()
        manager_info = getManagersInfo()
        employee_info = getSwStaff()
        # print staff_data
        print(from_date)
        logging.debug("From_date :" + str(from_date))
        print(to_date)
        logging.debug("To_date :" + str(to_date))
        # from_date = 1664521200
        # to_date = 1667285999
        dbobj = DBConnections.DataBaseConn()
        db = dbobj.connetReadDB()
        cur = db.cursor()
        for staff in staff_data:
            logging.debug(staff)
            if staff not in [
                2,
                15757,
                103,
                8331,
                18617,
                8341,
                14817,
                10707,
                10587,
                13167,
                320,
            ]:
                # if staff in [11547,12657,9422,5551,3441,9875]:
                # if staff in [4361]:
                # print staff
                cond = ""
                cond += " and b.staffid = (" + str(staff) + ")"
                try:
                    sql = "select a.ticketid,a.cid, a.mid, a.priority,a.noc,c.title as departmentname,d.email as owner , "
                    sql += "REPLACE(REPLACE(a.subject, '\r', ''), '\n', '') as Subject,nt.typename,a.devicetype,a.status, "
                    sql += "a.created_dt, "
                    sql += "b.dateline, "
                    sql += "d2.email as engineername, "
                    sql += "d2.fullname  as name, "
                    sql += "a.client, a.msp as partner,a.devicetype, "
                    sql += "a.client,a.device, "
                    sql += "e.timespent,  "
                    sql += "e.timebillable,  "
                    sql += "b.staffid, b.ticketpostid  "
                    sql += "from swticketposts b  "
                    sql += "join incident_data a on a.ticketid=b.ticketid  "
                    sql += "join swdepartments c on a.deptid=c.departmentid "
                    sql += "left join swstaff d on a.staffid=d.staffid "
                    sql += "join swstaff d2 on b.staffid=d2.staffid "
                    sql += "left join ntstickettype nt on nt.typeid=a.typeid "
                    sql += "join swtickettimetrack e on b.ticketid=e.ticketid and b.dateline=e.dateline  "
                    sql += (
                        "where b.dateline >= "
                        + str(from_date)
                        + " and b.dateline < "
                        + str(to_date)
                        + " and b.staffid > 0 "
                        + str(cond)
                        + "and e. Timespent > 0  order by b.dateline"
                    )

                    cur.execute(sql)
                    result = dictfetchall(cur)

                    if result:
                        logging.debug("result ====>" + str(len(result)))
                        for utilization_data in result:
                            # day_wise_data.append(res)
                            prepare_utilization_data = {}

                            # print utilization_data
                            # print utilization_data['name']

                            prepare_utilization_data["swstaff_id"] = int(
                                utilization_data["staffid"]
                            )
                            prepare_utilization_data["ticketid"] = int(
                                utilization_data["ticketid"]
                            )
                            prepare_utilization_data["ticketpostid"] = int(
                                utilization_data["ticketpostid"]
                            )
                            prepare_utilization_data["resource_name"] = str(
                                utilization_data["name"]
                            )
                            prepare_utilization_data["touched_date"] = int(
                                utilization_data["dateline"]
                            )
                            prepare_utilization_data["timespent"] = int(
                                utilization_data["timespent"]
                            )
                            prepare_utilization_data["billable_time"] = int(
                                utilization_data["timebillable"]
                            )

                            # Fetch nr_staffid and his manager_id by swstaffid
                            if utilization_data["staffid"]:
                                if employee_info:
                                    for emp_info in employee_info:
                                        if (
                                            emp_info["staffid"]
                                            == utilization_data["staffid"]
                                        ):
                                            # print "EMP INFO: ", emp_info
                                            prepare_utilization_data[
                                                "nrstaff_id"
                                            ] = int(emp_info["id"])
                                            reporting_mngr_id = emp_info[
                                                "reporting_manager_id"
                                            ]

                                            if manager_info:
                                                for mngr_info in manager_info:
                                                    # print "MNGR INFO: ", mngr_info
                                                    if (
                                                        mngr_info["id"]
                                                        == reporting_mngr_id
                                                    ):
                                                        prepare_utilization_data[
                                                            "manager_id"
                                                        ] = int(mngr_info["swstaff_id"])
                                                        prepare_utilization_data[
                                                            "manager_name"
                                                        ] = str(
                                                            mngr_info["staff_fullname"]
                                                        )

                            prepare_utilization_data["ticket_created_date"] = int(
                                utilization_data["created_dt"]
                            )
                            prepare_utilization_data["post_date"] = int(
                                utilization_data["dateline"]
                            )
                            prepare_utilization_data["subject"] = str(
                                utilization_data["Subject"]
                            )
                            prepare_utilization_data["partner_id"] = int(
                                utilization_data["mid"]
                            )
                            prepare_utilization_data["partner"] = str(
                                utilization_data["partner"]
                            )
                            prepare_utilization_data["client_id"] = int(
                                utilization_data["cid"]
                            )
                            prepare_utilization_data["client"] = str(
                                utilization_data["client"]
                            )
                            prepare_utilization_data["ticket_type"] = str(
                                utilization_data["typename"]
                            )
                            if str(utilization_data["device"]) != "":
                                prepare_utilization_data["device"] = str(
                                    utilization_data["device"]
                                )
                            else:
                                prepare_utilization_data["device"] = "NA"

                            if str(utilization_data["devicetype"]) != "":
                                prepare_utilization_data["devicetype"] = str(
                                    utilization_data["devicetype"]
                                )
                            else:
                                prepare_utilization_data["devicetype"] = "NA"

                            # print "\n Final data prepared user wise:", len(prepare_utilization_data)

                            insertRecord(
                                prepare_utilization_data, "utilization_report_data"
                            )

                except Exception as e:
                    print("Exception in Fetching dates ::" + str(e))
                    logging.debug("Exception in Fetching dates:: =>" + str(e))
        logging.debug("=============================================================")
        logging.debug("day_wise_data ::" + str(day_wise_data))
        logging.debug("=============================================================")
        return day_wise_data
        db.close()
        cur.close()
    except Exception as e:
        print("Exception in getUserUtilizationDataDaywise method ::" + str(e))
        logging.debug("Exception in getUserUtilizationDataDaywise method:: =>" + str(e))


def insertRecord(info, tableName):
    try:
        dbobj = DBConnections.DataBaseConn()
        db = dbobj.connetReadDB()
        cur = db.cursor()
        count = 0
        # print "inside insert record"
        placeholders = ", ".join(["%s"] * len(info))
        columns = ", ".join(info.keys())
        logging.debug("====================================ph")
        logging.debug("placeHolder :" + str(placeholders))
        logging.debug("columns :" + str(columns))
        sql = (
            "INSERT INTO "
            + str(tableName)
            + " ( %s ) VALUES ( %s )" % (columns, placeholders)
        )
        print(sql)
        cur.execute(sql, info.values())
        count = int(cur.rowcount)
        print(count)
        logging.debug(count)
        db.commit()
        if not count:
            logging.debug("Record not inserted")
        logging.debug("===========================================ph")
        return sql
    except Exception as e:
        # handle all your exceptions... this is just an example
        print(sql)
        print("Caught Exception: %s" % e)
        logging.debug("Exception in insertRecord method :" + str(e))


def main():
    logging.debug("==================================")
    try:
        tmp = fetchCurrentAndYesterDayForUtilReportUpdateData()

        details = fetchDetailsFromSwTicketPost(
            tmp[0]["yesterDayPST"], tmp[0]["currentTimeInPST"]
        )

        if details:
            insertDetailsIntoStaffPostupdateTime(details)

        getUserUtilizationDataDaywise(
            tmp[0]["yesterDayPST"], tmp[0]["currentTimeInPST"]
        )

    except Exception as e:
        print("Exception in Main Method::" + str(e))
        logging.debug("Exception in Main method=>" + str(e))


if __name__ == "__main__":
    main()

logging.debug(
    "---------------- Script  completed at:%s ------------------" % dt.datetime.utcnow()
)
