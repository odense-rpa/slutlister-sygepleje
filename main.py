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
from organizations import godkendte_organisationer

nexusklient = None
nexus_borgere = None
nexus_organisationer = None
nexus_kalender = None
nexus_opgaver = None
afregningsklient = None


async def populate_queue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    alle_organisationer = nexus_organisationer.get_organizations()

    for organisation in alle_organisationer:
        if organisation["name"] not in godkendte_organisationer:
            continue

        borgere = nexus_organisationer.get_citizens_by_organization(organisation)

        logger.info(
            f"Tilføjer {len(borgere)} borgere fra organisationen {organisation['name']}"
        )

        for borger in borgere:
            if borger["patientIdentifier"]["type"] != "cpr":
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
                logger.error(f"Fejl ved processering af item: {data}. Fejl: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    ats = AutomationServer.from_environment()

    workqueue = ats.workqueue()

    # Initialize external systems for automation here..
    credential = Credential.get_credential("KMD Nexus - produktion")
    afregnings_credential = Credential.get_credential("Odense SQL Server")

    nexusklient = NexusClient(
        instance=credential.get_data_as_dict()["instance"],
        client_secret=credential.password,
        client_id=credential.username,
    )

    nexus_borgere = CitizensClient(nexus_client=nexusklient)
    nexus_organisationer = OrganizationsClient(nexus_client=nexusklient)
    nexus_kalender = CalendarClient(
        nexus_client=nexusklient, citizens_client=nexus_borgere
    )
    nexus_opgaver = AssignmentsClient(nexus_client=nexusklient)

    afregningsklient = Tracker(
        username=afregnings_credential.username, password=afregnings_credential.password
    )

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue("new")
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
