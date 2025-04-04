import asyncio
import logging
import sys

from odk_tools.tracking import Tracker
from automation_server_client import (
    AutomationServer,
    Workqueue,
    WorkItemError,
    Credential,
)

from kmd_nexus_client import (
    NexusClient,
    CitizensClient,
    OrganizationsClient,
    CalendarClient,
    AssignmentsClient,
)

# temp fix since no Q:\
from organizations import approved_organizations

nexus_client = None
citizens_client = None
organizations_client = None
calendar_client = None
assignments_client = None
tracking_client = None

async def populate_queue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    all_organizations = organizations_client.get_all_organizations()

    for organization in all_organizations:
        if organization["name"] not in approved_organizations:
            continue

        citizens = organizations_client.get_citizens_by_organization(organization)

        logger.info(f"Adding {len(citizens)} citizens to from organization {organization['name']}")

        for citizen in citizens:
            if citizen["patientIdentifier"]["type"] != "cpr":
                continue

            

async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    logger.info("Hello from process workqueue!")

    for item in workqueue:
        with item:
            data = item.get_data_as_dict()

            try:
                # Process the item here
                pass
            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    ats = AutomationServer.from_environment()

    workqueue = ats.workqueue()

    # Initialize external systems for automation here..
    credential = Credential.get_credential("KMD Nexus - produktion")
    tracking_credential = Credential.get_credential("Odense SQL Server")

    nexus_client = NexusClient(
        instance=credential.get_data_as_dict()["instance"],
        client_secret=credential.password,
        client_id=credential.username
    )

    citizens_client = CitizensClient(nexus_client=nexus_client)
    organizations_client = OrganizationsClient(nexus_client=nexus_client)
    calendar_client = CalendarClient(nexus_client=nexus_client)
    assignments_client = AssignmentsClient(nexus_client=nexus_client)

    tracking_client = Tracker(
        username=tracking_credential.username,
        password=tracking_credential.password
    )


    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue("new")
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
