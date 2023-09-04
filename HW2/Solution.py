from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql
from Utility.DBConnector import ResultSet


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("Begin; CREATE TABLE Photo( "
                     "photo_id INTEGER PRIMARY KEY NOT NULL CHECK(photo_id > 0), "
                     "description TEXT NOT NULL, "
                     "photo_size INTEGER NOT NULL CHECK(photo_size >= 0));"

                     "CREATE TABLE Disk("
                     "disk_id INTEGER PRIMARY KEY NOT NULL CHECK(disk_id > 0),"
                     "disk_company TEXT NOT NULL,"
                     "disk_speed INTEGER NOT NULL CHECK(disk_speed> 0),"
                     "disk_free_space INTEGER NOT NULL CHECK(disk_free_space >= 0),"
                     "disk_CPB INTEGER NOT NULL CHECK(disk_CPB > 0));"

                     "CREATE TABLE RAM("
                     "RAM_id INTEGER PRIMARY KEY NOT NULL CHECK(RAM_id> 0),"
                     "RAM_size INTEGER NOT NULL CHECK(RAM_size> 0),"
                     "RAM_company TEXT NOT NULL );"

                     "CREATE TABLE DiskRAM("
                     "disk_id INTEGER,"
                     "RAM_id INTEGER, "
                     "FOREIGN KEY (disk_id) REFERENCES Disk(disk_id) ON DELETE CASCADE, "
                     "FOREIGN KEY (RAM_id) REFERENCES RAM(RAM_id) ON DELETE CASCADE, "
                     "PRIMARY KEY (disk_id, RAM_id)); "

                     "CREATE TABLE DiskPhoto("
                     "disk_id INTEGER,"
                     "photo_id INTEGER, "
                     "FOREIGN KEY (disk_id) REFERENCES Disk(disk_id) ON DELETE CASCADE, "
                     "FOREIGN KEY (photo_id) REFERENCES Photo(photo_id) ON DELETE CASCADE, "
                     "PRIMARY KEY (disk_id, photo_id));"

                     "COMMIT;")
        conn.commit()

    except Exception as e:
        print(e)

    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("TRUNCATE Photo, Disk, RAM, DiskRAM, DiskPhoto")
        conn.commit()

    except Exception as e:
        print(e)

    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DROP TABLE IF EXISTS Photo CASCADE;"
                     "DROP TABLE IF EXISTS Disk CASCADE;"
                     "DROP TABLE IF EXISTS RAM CASCADE;"
                     "DROP TABLE IF EXISTS DiskRAM CASCADE;"
                     "DROP TABLE IF EXISTS DiskPhoto CASCADE;"
                     "DROP TABLE IF EXISTS Temp CASCADE;"

                     "COMMIT")
        conn.commit()

    except Exception as e:
        print(e)

    finally:
        conn.close()


def addPhoto(photo: Photo) -> ReturnValue:
    conn = None
    try:
        photo_id = sql.Literal(photo.getPhotoID())
        description = sql.Literal(photo.getDescription())
        photo_size = sql.Literal(photo.getSize())

        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Photo(photo_id, description, photo_size) "
                        "VALUES ({photo_id},{description},{photo_size})").format(photo_id=photo_id,
                                                                                 description=description,
                                                                                 photo_size=photo_size)
        rows, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS

    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS

    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getPhotoByID(photoID: int) -> Photo:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Photo WHERE Photo.photo_id = {id}".format(id=photoID))
        conn.commit()

    except Exception as e:
        return Photo.badPhoto()

    finally:
        conn.close()
        if result.rows:
            return Photo(result.rows[0][0], result.rows[0][1], result.rows[0][2])
        return Photo.badPhoto()


def deletePhoto(photo: Photo) -> ReturnValue:
    conn = None
    try:
        photo_id = sql.Literal(photo.getPhotoID())
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "DROP VIEW IF EXISTS PHOTO_SIZE CASCADE; "
                        "DROP VIEW IF EXISTS DISK_HAVING_PHOTO CASCADE; "

                        "CREATE VIEW PHOTO_SIZE AS "
                        "SELECT photo_size "
                        "FROM Photo "
                        "WHERE photo_id = {photo_id}; "

                        "CREATE VIEW DISK_HAVING_PHOTO AS "
                        "SELECT disk_id "
                        "FROM DiskPhoto "
                        "WHERE photo_id = {photo_id}; "

                        "UPDATE Disk "
                        "SET disk_free_space = disk_free_space + (SELECT SUM(photo_size) FROM PHOTO_SIZE) "
                        "WHERE Disk.disk_id IN (SELECT disk_id FROM DISK_HAVING_PHOTO);"

                        "DELETE FROM Photo "
                        "WHERE  photo_id = {photo_id};"

                        "COMMIT;").format(photo_id=photo_id)
        conn.execute(query)
        conn.commit()

    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Disk(disk_id, disk_company, disk_speed, disk_free_space, disk_CPB) "
                        "VALUES({id}, {company}, {speed}, {free_space}, {CPB}) "
                        ).format(
            id=sql.Literal(disk.getDiskID()),
            company=sql.Literal(disk.getCompany()),
            speed=sql.Literal(disk.getSpeed()),
            free_space=sql.Literal(disk.getFreeSpace()),
            CPB=sql.Literal(disk.getCost()))

        rows_effected, _ = conn.execute(query)
        conn.commit()

    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS

    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS

    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getDiskByID(diskID: int) -> Disk:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Disk WHERE Disk.disk_id = {disk_id}".format(disk_id=diskID))
        conn.commit()

    except Exception as e:
        return Disk.badDisk()

    finally:
        conn.close()
        if result.rows:
            return Disk(result.rows[0][0], result.rows[0][1], result.rows[0][2], result.rows[0][3], result.rows[0][4])
        return Disk.badDisk()


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()

    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disk "
                        "WHERE  Disk.disk_id = {disk_id} ").format(disk_id=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()

    if rows_effected == 0:
        return ReturnValue.NOT_EXISTS

    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    try:
        id_Ram = sql.Literal(ram.getRamID())
        size_Ram = sql.Literal(ram.getSize())
        company_Ram = sql.Literal(ram.getCompany())
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RAM(RAM_id, RAM_size, RAM_company) "
                        "VALUES({id_Ram}, {size_Ram}, {company_Ram})").format(id_Ram=id_Ram, size_Ram=size_Ram,
                                                                              company_Ram=company_Ram)

        rows_count, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getRAMByID(ramID: int) -> RAM:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM RAM WHERE RAM.RAM_id = {id}".format(id=ramID))
        conn.commit()

    except Exception as e:
        return Photo.BadPhoto()

    finally:
        conn.close()
        if result.rows:
            return RAM(result.rows[0][0], result.rows[0][2], result.rows[0][1])
        return RAM.badRAM()


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    rows_count, answer = 0, Connector.ResultSet()

    try:
        id_Ram = sql.Literal(ramID)
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RAM "
                        "WHERE  RAM.RAM_id = {id_Ram} ").format(id_Ram=id_Ram)
        rows_count, answer = conn.execute(query)
        conn.commit()

    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    if rows_count == 0:
        return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO Photo(photo_id, description, photo_size) "
                        "VALUES({photo_id}, {description}, {photo_size});"
                        "INSERT INTO Disk(disk_id, disk_company, disk_speed, disk_free_space, disk_CPB) "
                        "VALUES({id_disk1}, {company_disk1}, {speed_disk1}, {free_space_disk1}, {CPB_disk1});"
                        "COMMIT;").format(photo_id=sql.Literal(photo.getPhotoID()),
                                          description=sql.Literal(photo.getDescription()),
                                          photo_size=sql.Literal(photo.getSize()),
                                          id_disk1=sql.Literal(disk.getDiskID()),
                                          company_disk1=sql.Literal(disk.getCompany()),
                                          speed_disk1=sql.Literal(disk.getSpeed()),
                                          free_space_disk1=sql.Literal(disk.getFreeSpace()),
                                          CPB_disk1=sql.Literal(disk.getCost()))

        rows_effected, _ = conn.execute(query)
        conn.commit()

    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS

    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS

    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()
    return ReturnValue.OK


def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    connector = None
    try:
        disk_id = sql.Literal(diskID)
        photo_id = sql.Literal(photo.getPhotoID())
        connector = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO DiskPhoto(disk_id, photo_id) "
                        "VALUES({disk_id} ,{photo_id});"

                        "DELETE FROM DiskPhoto "
                        "WHERE  DiskPhoto.disk_id = {disk_id} "
                        "AND  DiskPhoto.photo_id = {photo_id};"

                        "INSERT INTO DiskPhoto(photo_id,disk_id) "
                        "(SELECT Photo.photo_id , Disk.disk_id "
                        "FROM Photo, Disk "
                        "WHERE  Disk.disk_id = {disk_id}"
                        "AND  Photo.photo_id = {photo_id}"
                        "AND  Photo.photo_size <= Disk.disk_free_space); "

                        "UPDATE Disk "
                        "SET disk_free_space= disk_free_space - "

                        "(SELECT SUM(Photo.photo_size)"
                        "FROM Photo "
                        "WHERE Photo.photo_id = {photo_id})"

                        "WHERE Disk.disk_id = {disk_id}"
                        "AND EXISTS "
                        "(SELECT photo_id FROM Photo "
                        "WHERE Photo.photo_id = {photo_id});"

                        "COMMIT;").format(disk_id=disk_id, photo_id=photo_id)
        rows_count, _ = connector.execute(query)
        connector.commit()

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        connector.close()
    return ReturnValue.OK


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"

                        "UPDATE Disk "
                        "SET disk_free_space= disk_free_space + "
                        "(SELECT SUM(Photo.photo_size)"
                        "FROM Photo "
                        "WHERE Photo.photo_id = {photo_id})"
                        "WHERE Disk.disk_id = {disk_id}"
                        "AND EXISTS "
                        "(SELECT * FROM DiskPhoto "
                        "WHERE DiskPhoto.photo_id = {photo_id} AND DiskPhoto.disk_id = {disk_id});"

                        "DELETE FROM DiskPhoto "
                        "WHERE  photo_id = {photo_id} AND disk_id = {disk_id} ;"

                        "COMMIT;").format(disk_id=sql.Literal(diskID), photo_id=sql.Literal(photo.getPhotoID()))
        rows_effected, _ = conn.execute(query)
        conn.commit()

    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()

    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    connector = None
    try:
        disk_id = sql.Literal(diskID)
        ram_id = sql.Literal(ramID)
        connector = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO DiskRAM(disk_id, RAM_id) "
                        "VALUES({disk_id} ,{ram_id});"

                        "DELETE FROM DiskRAM "
                        "WHERE  DiskRAM.disk_id = {disk_id} "
                        "AND  DiskRAM.ram_id = {ram_id};"

                        "INSERT INTO DiskRAM(disk_id,RAM_id) "
                        "(SELECT Disk.disk_id, RAM.RAM_id  "
                        "FROM Disk, RAM "
                        "WHERE  Disk.disk_id = {disk_id} AND RAM.RAM_id = {ram_id}); "
                        "COMMIT;").format(disk_id=disk_id, ram_id=ram_id)

        rows_effected, _ = connector.execute(query)
        connector.commit()

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        connector.close()
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM DiskRAM "
                        "WHERE  ram_id = {ram_id} AND disk_id = {disk_id} ;").format(disk_id=sql.Literal(diskID),
                                                                                     ram_id=sql.Literal(ramID))

        rows_effected, _ = conn.execute(query)
        conn.commit()

    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()

    if rows_effected > 0:
        return ReturnValue.OK

    else:
        return ReturnValue.NOT_EXISTS


def averagePhotosSizeOnDisk(diskID: int) -> float:
    connector = None
    answer = None
    try:
        disk_id = sql.Literal(diskID)
        connector = Connector.DBConnector()
        query = sql.SQL("SELECT AVG(photo_size) "
                        "FROM Photo "
                        "WHERE Photo.photo_id "
                        "IN (SELECT photo_id "
                        "FROM DiskPhoto "
                        "WHERE disk_id= {disk_id}"
                        "GROUP BY disk_id, photo_id "
                        "HAVING disk_id = {disk_id})").format(disk_id=disk_id)

        rows_count, answer = connector.execute(query)
        connector.commit()

    except Exception as e:
        return -1
    finally:
        connector.close()
    if answer.rows[0][0] is None:
        return 0
    return float(answer.rows[0][0])


def getTotalRamOnDisk(diskID: int) -> int:
    conn = None
    X = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM(RAM_size) "
                        "FROM RAM "
                        "WHERE RAM.RAM_id IN "
                        "(SELECT RAM_id "
                        "FROM DiskRAM "
                        "WHERE disk_id= {disk_id}"
                        "GROUP BY disk_id, RAM_id "
                        "HAVING disk_id = {disk_id})").format(disk_id=sql.Literal(diskID))
        rows_effected, X = conn.execute(query)
        conn.commit()

    except Exception as e:
        return -1
    finally:
        conn.close()

    if X.rows[0][0] is None:
        return 0

    return X.rows[0][0]


def getCostForDescription(description: str) -> int:
    connector = None
    X = None
    try:
        connector = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "DROP VIEW IF EXISTS Photo_id_size CASCADE; "
                        "DROP VIEW IF EXISTS Disk_id_CPB CASCADE; "
                        "DROP VIEW IF EXISTS DiskIDS2 CASCADE; "

                        "CREATE VIEW Photo_id_size AS "
                        "SELECT photo_id ,photo_size "
                        "FROM Photo "
                        "GROUP BY description, photo_id "
                        "HAVING description = {description};"

                        "CREATE VIEW Disk_id_CPB AS "
                        "SELECT  disk_id ,disk_CPB "
                        "FROM Disk; "

                        "CREATE VIEW DiskIDS2 AS "
                        "SELECT photo_size , disk_CPB "
                        "FROM ((DiskPhoto INNER JOIN Photo_id_size  "
                        "ON DiskPhoto.photo_id = Photo_id_size.photo_id)"
                        "INNER JOIN Disk_id_CPB ON DiskPhoto.disk_id = Disk_id_CPB.disk_id); "

                        "SELECT SUM(photo_size* disk_CPB) "
                        "FROM DiskIDS2 "
                        "COMMIT;").format(description=sql.Literal(description))

        rows_count, X = connector.execute(query)
        connector.commit()

    except Exception as e:
        return -1
    finally:
        connector.close()

    if X.rows[0][0] is None:
        return 0
    return X.rows[0][0]


def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT photo_id "
                        "FROM Photo "
                        "WHERE Photo.photo_size <= (SELECT SUM(disk_free_space) "
                        "FROM Disk "
                        "WHERE Disk.disk_id = {disk_id}) "
                        "ORDER BY photo_id DESC LIMIT 5").format(disk_id=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return []

    finally:
        conn.close()

    return_list = []
    for x in result.rows:
        return_list.append(int(x[0]))
    return return_list


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "DROP VIEW IF EXISTS RAM_PART_OF_DISK CASCADE; "
                        "DROP VIEW IF EXISTS RAM_SIZED CASCADE; "
                        "DROP VIEW IF EXISTS DISK_FREE_SPACE CASCADE; "
                        "DROP VIEW IF EXISTS RESULT CASCADE; "
                        "DROP VIEW IF EXISTS RESULT_2 CASCADE; "


                        "CREATE VIEW RAM_PART_OF_DISK AS "
                        "SELECT RAM_id "
                        "FROM DiskRAM "
                        "GROUP BY disk_id, RAM_id "
                        "HAVING disk_id = {disk_id};"

                        "CREATE VIEW RAM_SIZED AS "
                        "SELECT RAM_size "
                        "FROM RAM INNER JOIN RAM_PART_OF_DISK "
                        "ON RAM.RAM_id = RAM_PART_OF_DISK.RAM_id; "

                        "CREATE VIEW DISK_FREE_SPACE AS "
                        "SELECT disk_free_space "
                        "FROM Disk "
                        "WHERE disk_id = {disk_id}; "

                        "CREATE VIEW RESULT AS SELECT photo_id FROM Photo "
                        "WHERE photo_size <= COALESCE((SELECT SUM(disk_free_space) FROM DISK_FREE_SPACE),-1) "
                        "AND photo_size <= COALESCE((SELECT SUM(RAM_size) FROM RAM_SIZED),0); "

                        "CREATE VIEW RESULT_2 AS "
                        "SELECT photo_id "
                        "FROM RESULT "
                        "ORDER BY photo_id ASC LIMIT 5;"

                        "SELECT * FROM RESULT_2 "

                        "COMMIT;").format(disk_id=sql.Literal(diskID))

        rows_effected, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return []

    finally:
        conn.close()

    answer_list = []
    if result.rows:
        for x in result.rows:
            answer_list.append(int(x[0]))

    return answer_list


def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "DROP TABLE IF EXISTS Temp CASCADE; "
                        "DROP VIEW IF EXISTS RAM_IN_DISK_COMPANY CASCADE; "
                        "DROP VIEW IF EXISTS DISK_COMPANY_NAME CASCADE; "
                        "DROP VIEW IF EXISTS RESULT CASCADE; "

                        "CREATE TABLE Temp(disk_id INTEGER, FOREIGN KEY (disk_id) REFERENCES Disk(disk_id));"

                        "INSERT INTO Temp(disk_id) VALUES({disk_id});"

                        "DROP TABLE IF EXISTS Temp ;"

                        "CREATE VIEW DISK_COMPANY_NAME AS SELECT disk_company "
                        "FROM Disk WHERE Disk.disk_id = {disk_id};"

                        "CREATE VIEW RAM_IN_DISK_COMPANY AS SELECT DISTINCT RAM_company "
                        "FROM RAM WHERE RAM.RAM_id IN "
                        "(SELECT  RAM_id FROM DiskRAM GROUP BY disk_id, RAM_id HAVING disk_id = {disk_id}); "

                        "CREATE VIEW RESULT AS SELECT disk_company "
                        "FROM DISK_COMPANY_NAME, RAM_IN_DISK_COMPANY "
                        "WHERE DISK_COMPANY_NAME.disk_company != RAM_IN_DISK_COMPANY.RAM_company;"

                        "SELECT COUNT(disk_company) FROM RESULT "
                        "COMMIT;").format(disk_id=sql.Literal(diskID))

        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return False
    except Exception as e:
        return False

    finally:
        conn.close()

    if result.rows[0][0] != 0:
        return False

    return True


def isDiskContainingAtLeastNumExists(description: str, num: int) -> bool:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "DROP VIEW IF EXISTS MATCH_PHOTOS CASCADE; "
                        "DROP VIEW IF EXISTS MATCH_DISKS CASCADE; "

                        "CREATE VIEW MATCH_PHOTOS AS "
                        "SELECT photo_id "
                        "FROM Photo "
                        "WHERE description = {description};"

                        "CREATE VIEW MATCH_DISKS AS "
                        "SELECT disk_id "
                        "FROM DiskPhoto INNER JOIN MATCH_PHOTOS "
                        "ON DiskPhoto.photo_id = MATCH_PHOTOS.photo_id;"

                        "SELECT disk_id "
                        "FROM MATCH_DISKS "
                        "GROUP BY disk_id "
                        "HAVING COUNT(disk_id) >= {num} ").format(description=sql.Literal(description), num=sql.Literal(num))


        rows_effected, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return False
    finally:
        conn.close()
    if result.rows:
        return True
    return False


def getDisksContainingTheMostData() -> List[int]:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT Disk.disk_id, COALESCE(SUM(Photo.photo_size),0) AS total_size "
                        "FROM Disk LEFT JOIN DiskPhoto " 
                        "ON Disk.disk_id = DiskPhoto.disk_id " 
                        "LEFT JOIN Photo " 
                        "ON DiskPhoto.photo_id = Photo.photo_id "
                        "GROUP BY Disk.disk_id "
                        "ORDER BY total_size DESC ,disk_id ASC "
                        "LIMIT 5; ")

        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return []
    finally:
        conn.close()

        ret_list = []
    if result.rows:
        for res in result.rows:
            ret_list.append(int(res[0]))
        return ret_list
    return []


def getConflictingDisks() -> List[int]:
    conn = None
    rows_count, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "DROP VIEW IF EXISTS PHOTO_COUNT_OF_DISKS CASCADE; "
                        "DROP VIEW IF EXISTS PHOTO_COUNT_GREATER_THAN_ONE CASCADE; "
                        "DROP VIEW IF EXISTS RESULT CASCADE; "

                        "CREATE VIEW PHOTO_COUNT_OF_DISKS AS "
                        "SELECT photo_id, COUNT(disk_id) "
                        "FROM DiskPhoto "
                        "GROUP BY DiskPhoto.photo_id; "

                        "CREATE VIEW PHOTO_COUNT_GREATER_THAN_ONE AS "
                        "SELECT photo_id "
                        "FROM PHOTO_COUNT_OF_DISKS "
                        "WHERE PHOTO_COUNT_OF_DISKS.count>1; "

                        "CREATE VIEW RESULT AS "
                        "SELECT DISTINCT disk_id "
                        "FROM DiskPhoto "
                        "WHERE DiskPhoto.photo_id IN (SELECT photo_id FROM PHOTO_COUNT_GREATER_THAN_ONE) "
                        "ORDER BY disk_id ASC;"

                        "SELECT * "
                        "FROM RESULT "

                        "COMMIT;")

        rows_count, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return []

    finally:
        conn.close()

    answer_list = []

    if result.rows:
        for res in result.rows:
            answer_list.append(int(res[0]))
        return answer_list
    return []


def mostAvailableDisks() -> List[int]:
    conn = None
    rows_count, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "DROP VIEW IF EXISTS DISK_ID_DISK_SPEED_NUMBER_OF_PHOTOS CASCADE; "
                        "DROP VIEW IF EXISTS RESULT CASCADE; "

                        "CREATE VIEW DISK_ID_DISK_SPEED_NUMBER_OF_PHOTOS AS "
                        "SELECT disk_id, disk_speed, count(photo_id) "
                        "FROM Disk LEFT JOIN Photo ON Disk.disk_free_space >= Photo.photo_size "
                        "GROUP BY Disk.disk_id; "

                        "CREATE VIEW RESULT AS "
                        "SELECT disk_id, disk_speed, DISK_ID_DISK_SPEED_NUMBER_OF_PHOTOS.count "
                        "FROM DISK_ID_DISK_SPEED_NUMBER_OF_PHOTOS "
                        "GROUP BY disk_id, disk_speed ,DISK_ID_DISK_SPEED_NUMBER_OF_PHOTOS.count "
                        "ORDER BY count DESC, disk_speed DESC, disk_id ASC LIMIT 5; "

                        "SELECT disk_id FROM RESULT "
                        "COMMIT;")

        rows_count, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return []
    finally:
        conn.close()
    answer_list = []
    if (result.rows):
        for res in result.rows:
            answer_list.append(int(res[0]))
        return answer_list
    return []


def getClosePhotos(photoID: int) -> List[int]:
    conn = None
    rows_count, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "DROP VIEW IF EXISTS PhotosInDisk CASCADE; "
                        "DROP VIEW IF EXISTS allPhotos CASCADE; "
                        "DROP VIEW IF EXISTS CountCommon CASCADE; "
                        "DROP VIEW IF EXISTS Count2 CASCADE; "
                        "DROP VIEW IF EXISTS comb CASCADE; "
                        "DROP VIEW IF EXISTS Count3 CASCADE; "
                        "DROP VIEW IF EXISTS returnVal CASCADE; "
                        "DROP TABLE IF EXISTS Temp CASCADE; "

                        "CREATE TABLE Temp(photo_id INTEGER, FOREIGN KEY (photo_id) REFERENCES Photo(photo_id));"

                        "INSERT INTO Temp(photo_id) VALUES({photo_id});"

                        "DROP TABLE IF EXISTS Temp ;"

                        "CREATE VIEW PhotosInDisk AS SELECT photo_id,disk_id "
                        "FROM DiskPhoto "
                        "GROUP BY photo_id,disk_id "
                        "HAVING photo_id = {photo_id}; "

                        "CREATE VIEW allPhotos AS SELECT photo_id, 0 count "
                        "FROM Photo "
                        "WHERE photo_id !={photo_id} ;"

                        "CREATE VIEW CountCommon AS SELECT photo_id,COUNT(disk_id) FROM DiskPhoto "
                        "GROUP BY photo_id ,disk_id "
                        "HAVING DiskPhoto.disk_id IN (SELECT disk_id FROM PhotosInDisk) AND photo_id !={photo_id} ;"

                        "CREATE VIEW Count2 AS SELECT CountCommon.photo_id, COUNT(count) FROM CountCommon "
                        "GROUP BY CountCommon.photo_id ;"

                        "CREATE VIEW comb AS SELECT Distinct allPhotos.photo_id, "
                        "COALESCE((SELECT count FROM Count2 WHERE allPhotos.photo_id = Count2.photo_id),0) count "
                        "FROM allPhotos ;"

                        "CREATE VIEW Count3 AS SELECT photo_id FROM comb "
                        "WHERE count >= (SELECT count(disk_id)/2.0 FROM PhotosInDisk); "

                        "CREATE VIEW returnVal AS SELECT photo_id FROM Count3 "
                        "ORDER BY photo_id ASC LIMIT 10;"
                        "SELECT * FROM returnVal  "
                        "COMMIT;").format(photo_id=sql.Literal(photoID))

        rows_count, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return []

    finally:
        conn.close()
    answer_list = []

    if result.rows:
        for res in result.rows:
            answer_list.append(int(res[0]))
        return answer_list
    return []


if __name__ == '__main__':
    dropTables()
    
