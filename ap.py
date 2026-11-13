import os
import time
import json
import logging
import hashlib
import psycopg2
import requests
import traceback
from datetime import datetime,timedelta
from dotenv import load_dotenv
load_dotenv()
logger = logging.getLogger("ElectionLogger")
logger.setLevel(logging.DEBUG)

# HEADERS = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.8040.2 Safari/537.36'
# }
HEADERS = {
    "User-Agent": "ElectionResultsMonitor/1.0 (contact: micpowers98@proton.me)"
}

class AP:
    def __init__(self,skip_metadata=False):
        self.metadata = None
        self.progress = None

        self.metadata_link:str = os.getenv("METADATA_LINK","")
        self.progress_link:str = os.getenv("PROGRESS_LINK","")

        self.session = requests.Session()
        self.session.headers.update(HEADERS)

        self.connection_params = {
                'dbname': os.getenv("DATABASE"),
                'user': os.getenv("SQL_USERNAME"),
                'password': os.getenv("SQL_PASSWORD"),
                'host': os.getenv("HOST"),
                'port': os.getenv("PORT")
            }
        self.connection = psycopg2.connect(**self.connection_params)
        self.cursor = self.connection.cursor()

        self.get_metadata(skip_metadata)
        self.get_progress()

    def upload_metadata(self,**kwargs):
        try:
            self.cursor.execute(f"""
                CALL {os.getenv("HDR_SP","")}(
                    %s::varchar,
                    %s::varchar,
                    %s::varchar,
                    %s::varchar,
                    %s::varchar,
                    %s::varchar,
                    %s::varchar,
                    %s::varchar,
                    %s::date,
                    %s::varchar,
                    %s::boolean,
                    %s::integer,
                    %s::integer,
                    %s::integer
                )
                """,
                (
                    kwargs.get("raceID"),
                    kwargs.get("eventID"),
                    kwargs.get("raceType"),
                    kwargs.get("raceTypeID"),
                    kwargs.get("tabulationStatus"),
                    kwargs.get("raceCallStatus"),
                    kwargs.get("officeID"),
                    kwargs.get("officeName"),
                    kwargs.get("electionDate"),
                    kwargs.get("statePostal"),
                    kwargs.get("isCertified"),
                    kwargs.get("totalRegisteredVoters"),
                    kwargs.get("expectedVoteCount"),
                    kwargs.get("totalVoteCount")
                )
            )
            self.connection.commit()
        except psycopg2.Error:
            logger.error(f"""{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}|ERROR|{traceback.format_exc()}""")

    def upload_progress(self,**kwargs):
        try:
            self.cursor.execute(
                """
                CALL public."Race_DTL_Upsert"(
                    %s::varchar,
                    %s::varchar,
                    %s::varchar,
                    %s::integer,
                    %s::timestamp,
                    %s::varchar,
                    %s::integer,
                    %s::timestamp,
                    %s::varchar,
                    %s::timestamp,
                    %s::integer,
                    %s::integer,
                    %s::double precision,
                    %s::varchar,
                    %s::varchar,
                    %s::integer,
                    %s::integer,
                    %s::integer,
                    %s::integer,
                    %s::integer,
                    %s::double precision,
                    %s::varchar,
                    %s::varchar,
                    %s::integer,
                    %s::integer,
                    %s::integer,
                    %s::integer,
                    %s::integer,
                    %s::double precision,
                    %s::varchar,
                    %s::integer,
                    %s::integer,
                    %s::integer,
                    %s::integer,
                    %s::integer,
                    %s::double precision,
                    %s::text,
                    %s::varchar
                )
                """,
                (
                    kwargs.get("raceID"),
                    kwargs.get("statePostal"),
                    kwargs.get("stateName"),
                    kwargs.get("registeredVoters"),
                    kwargs.get("raceCallDateTime"),
                    kwargs.get("reportingUnitID"),
                    kwargs.get("reportingUnitLevel"),
                    kwargs.get("pollClosingTime"),
                    kwargs.get("level"),
                    kwargs.get("lastUpdated"),
                    kwargs.get("precinctsReported"),
                    kwargs.get("totalPrecincts"),
                    kwargs.get("estimatedElectionVotePct"),
                    kwargs.get("voteResultsType"),

                    kwargs.get("candidate1ID"),
                    kwargs.get("candidate1VoteCount"),
                    kwargs.get("candidate1ElectionDayInPersonVotes"),
                    kwargs.get("candidate1EarlyInPersonVotes"),
                    kwargs.get("candidate1MailInVotes"),
                    kwargs.get("candidate1EarlyVoteTotal"),
                    kwargs.get("candidate1VotePct"),
                    kwargs.get("candidate1Winner"),

                    kwargs.get("candidate2ID"),
                    kwargs.get("candidate2VoteCount"),
                    kwargs.get("candidate2ElectionDayInPersonVotes"),
                    kwargs.get("candidate2EarlyInPersonVotes"),
                    kwargs.get("candidate2MailInVotes"),
                    kwargs.get("candidate2EarlyVoteTotal"),
                    kwargs.get("candidate2VotePct"),

                    kwargs.get("candidate3ID"),
                    kwargs.get("candidate3VoteCount"),
                    kwargs.get("candidate3ElectionDayInPersonVotes"),
                    kwargs.get("candidate3EarlyInPersonVotes"),
                    kwargs.get("candidate3MailInVotes"),
                    kwargs.get("candidate3EarlyVoteTotal"),
                    kwargs.get("candidate3VotePct"),

                    kwargs.get("rawAPData"),
                    kwargs.get("rawAPDataHash")
                )
            )
            self.connection.commit()
        except psycopg2.Error:
            logger.error(f"""{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}|ERROR|{traceback.format_exc()}""")

    def upload_candidates(self,**kwargs):
        try:
            self.cursor.execute(
                """
                CALL public."Candidates_Upsert"(
                    %s::varchar,
                    %s::varchar,
                    %s::varchar,
                    %s::varchar,
                    %s::varchar
                )
                """,
                (
                    kwargs.get("clientID"),
                    kwargs.get("raceID"),
                    kwargs.get("firstName"),
                    kwargs.get("lastName"),
                    kwargs.get("party")
                )
            )

            self.connection.commit()
        except psycopg2.Error:
            logger.error(f"""{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}|ERROR|{traceback.format_exc()}""")

    def get_metadata(self,skip_metadata:bool):
        if skip_metadata:
            return
        try:
            r = self.session.get(
                self.metadata_link,
                timeout=30
            )

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")

                logger.warning(
                    f"{datetime.now().isoformat()}|"
                    f"AP returned 429|Retry-After={retry_after}"
                )
                return

            r.raise_for_status()
            self.metadata = r.json()

        except requests.exceptions.JSONDecodeError:
            logger.error(
                f"{datetime.now().isoformat()}|ERROR|"
                f"{traceback.format_exc()}"
            )

        except requests.exceptions.RequestException:
            logger.error(
                f"{datetime.now().isoformat()}|ERROR|"
                f"{traceback.format_exc()}"
            )

    def get_progress(self):
        try:
            r = self.session.get(
                self.progress_link,
                timeout=30
            )

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")

                logger.warning(
                    f"{datetime.now().isoformat()}|"
                    f"AP returned 429|Retry-After={retry_after}"
                )
                return

            r.raise_for_status()
            self.progress = r.json()

        except requests.exceptions.JSONDecodeError:
            logger.error(
                f"{datetime.now().isoformat()}|ERROR|"
                f"{traceback.format_exc()}"
            )

        except requests.exceptions.RequestException:
            logger.error(
                f"{datetime.now().isoformat()}|ERROR|"
                f"{traceback.format_exc()}"
            )

    def close(self):
        try:
            self.cursor.close()
        finally:
            self.connection.close()
            self.session.close()


def main():
    from logging.handlers import TimedRotatingFileHandler
    file_handler = TimedRotatingFileHandler(
        "/opt/election/app.log",
        when="midnight",
        interval=1,
        backupCount=90
    )
    logger.addHandler(file_handler)
    start_datetime = datetime.strptime(os.getenv("EXTRACTION_START_DATE","2050-01-01"),"%Y-%m-%d")
    current_datetime:datetime = datetime.now()
    end_datetime = datetime.strptime(os.getenv("EXTRACTION_STOP_DATE","2050-01-01"),"%Y-%m-%d")
    if current_datetime < start_datetime:
        total_sleep_time:timedelta = start_datetime - current_datetime
        total_sleep_seconds:float = total_sleep_time.total_seconds()
        total_sleep_minutes:float = total_sleep_seconds/60
        total_sleep_hours:float = total_sleep_minutes/60
        total_sleep_days:float = total_sleep_hours/24
        inner_sleep_hours:int = int((total_sleep_days - int(total_sleep_days)) * 24)
        inner_sleep_minutes:int = int((total_sleep_hours - int(total_sleep_hours)) * 60)
        inner_sleep_seconds:int = int((total_sleep_minutes - int(total_sleep_minutes)) * 60)
        logger.info(f"""{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}|INFO|Process started too soon. Extraction start date is {start_datetime.strftime("%Y-%m-%d")} 00:00:00. Needs to sleep for {int(total_sleep_days)} days, {inner_sleep_hours} hours, {inner_sleep_minutes} minutes and {inner_sleep_seconds} seconds.""")
        time.sleep(total_sleep_seconds)
    current_datetime:datetime = datetime.now()
    ap = AP()
    last_metadata_read = time.time()

    while current_datetime >= start_datetime and current_datetime < end_datetime:
        start = time.time()
        if time.time() - last_metadata_read >= 900:
            ap.get_metadata(skip_metadata=False)
            last_metadata_read = time.time()
        ap.get_progress()

        for key,item in ap.metadata.items():
            if "9999" in key:
                continue
            try:
                ap.upload_metadata(
                    raceID=item.get("raceID"),
                    eventID=item.get("eventID"),
                    raceType=item.get("raceType"),
                    raceTypeID=item.get("raceTypeID"),
                    tabulationStatus=item.get("tabulationStatus"),
                    raceCallStatus=item.get("raceCallStatus"),
                    officeID=item.get("officeID"),
                    officeName=item.get("officeName"),
                    electionDate=item.get("electionDate"),
                    statePostal=item.get("statePostal"),
                    isCertified=item.get("isCertified"),
                    totalRegisteredVoters=item.get("parameters",{"vote":{"registered":-1}}).get("vote").get("registered"),
                    expectedVoteCount=item.get("parameters",{"vote":{"expected":-1}}).get("vote").get("expected").get("actual"),
                    totalVoteCount=item.get("parameters",{"vote":{"total":-1}}).get("vote").get("total")
                )
                candidates = item.get("candidates",{})
                for candidate_key,candidate_value in candidates.items():
                    ap.upload_candidates(
                        clientID=candidate_key,
                        raceID = item.get("raceID"),
                        firstName=candidate_value.get("first","Ghost"),
                        lastName=candidate_value.get("last",candidate_value.get("party","Unknown")),
                        party=candidate_value.get("party","Unknown")
                    )
            except Exception:
                logger.error(f"""{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}|ERROR|{traceback.format_exc()}""")

        for key,item in ap.progress.items():
            if "9999" in key:
                continue
            try:
                rawData = json.dumps(item)
                candidates = item.get("candidates",[])
                len_candidates = len(candidates)
                ap.upload_progress(
                    raceID=item.get("raceID"),
                    statePostal=item.get("statePostal"),
                    stateName=item.get("stateName"),
                    registeredVoters=item.get("parameters",{"vote":{"registered":-1}}).get("vote").get("registered"),
                    raceCallDateTime=candidates[0].get("calledAt",None),
                    reportingUnitID=item.get("reportingunitID"),
                    reportingUnitLevel=item.get("reportingunitLevel"),
                    pollClosingTime=item.get("pollClosingTime"),
                    level=item.get("level"),
                    lastUpdated=item.get("lastUpdated"),
                    precinctsReported=item.get("precinctsReporting"),
                    totalPrecincts=item.get("precinctsTotal"),
                    estimatedElectionVotePct=item.get("eevp"),
                    voteResultsType=item.get("resultsType"),

                    candidate1ID=candidates[0].get("candidateID",None) if len_candidates >= 1 else None,
                    candidate1VoteCount=candidates[0].get("voteCount",None) if len_candidates >= 1 else None,
                    candidate1ElectionDayInPersonVotes=candidates[0].get("electionDayInPerson",None) if len_candidates >= 1 else None,
                    candidate1EarlyInPersonVotes=candidates[0].get("earlyInPerson",None) if len_candidates >= 1 else None,
                    candidate1MailInVotes=candidates[0].get("absenteeMail",None) if len_candidates >= 1 else None,
                    candidate1EarlyVoteTotal=candidates[0].get("advanceTotal",None) if len_candidates >= 1 else None,
                    candidate1VotePct=candidates[0].get("votePct",None) if len_candidates >= 1 else None,
                    candidate1Winner=candidates[0].get("winner",None) if len_candidates >= 1 else None,

                    candidate2ID=candidates[1].get("candidateID",None) if len_candidates >= 2 else None,
                    candidate2VoteCount=candidates[1].get("voteCount",None) if len_candidates >= 2 else None,
                    candidate2ElectionDayInPersonVotes=candidates[1].get("electionDayInPerson",None) if len_candidates >= 2 else None,
                    candidate2EarlyInPersonVotes=candidates[1].get("earlyInPerson",None) if len_candidates >= 2 else None,
                    candidate2MailInVotes=candidates[1].get("absenteeMail",None) if len_candidates >= 2 else None,
                    candidate2EarlyVoteTotal=candidates[1].get("advanceTotal",None) if len_candidates >= 2 else None,
                    candidate2VotePct=candidates[1].get("votePct",None) if len_candidates >= 2 else None,

                    candidate3ID=candidates[2].get("candidateID",None) if len_candidates >= 3 else None,
                    candidate3VoteCount=candidates[2].get("voteCount",None) if len_candidates >= 3 else None,
                    candidate3ElectionDayInPersonVotes=candidates[2].get("electionDayInPerson",None) if len_candidates >= 3 else None,
                    candidate3EarlyInPersonVotes=candidates[2].get("earlyInPerson",None) if len_candidates >= 3 else None,
                    candidate3MailInVotes=candidates[2].get("absenteeMail",None) if len_candidates >= 3 else None,
                    candidate3EarlyVoteTotal=candidates[2].get("advanceTotal",None) if len_candidates >= 3 else None,
                    candidate3VotePct=candidates[2].get("votePct",None) if len_candidates >= 3 else None,

                    rawAPData=rawData,
                    rawAPDataHash=hashlib.sha3_512(rawData.encode()).hexdigest()
                )
            except Exception:
                logger.error(f"""{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}|ERROR|{traceback.format_exc()}""")

        logger.info(f"""{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}|INFO|Election data successfully updated""")
        end = time.time()
        time.sleep(max(10,60 - (end - start)))
        current_datetime:datetime = datetime.now()
    ap.close()

if __name__ == "__main__":
    main()
