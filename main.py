import asyncio
import logging
import sys

from datetime import date, timedelta, datetime

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
    GrantsClient,
    filter_references,
)

# temp fix since no Q:\
from forløbsindplacering import forløbsindplacering
from workflow_states import godkendte_states
from organizations import godkendte_organisationer
from indsatser import godkendte_indsatser

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

        # logger.info(
        #     f"Tilføjer {len(borgere)} borgere fra organisationen {organisation['name']}"
        # )

        # 
        
        for borger in borgere:
            try:
                # skip Nancy
                if borger["patientIdentifier"]["identifier"] == "251248-9996":
                    continue

                cpr = borger["patientIdentifier"]["identifier"]
                workqueue.add_item({}, cpr)
            except Exception as e:
                logger.error(f"Fejl ved tilføjelse af borger {borger}: {e}")


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    for item in workqueue:
        with item:
            data = item.get_data_as_dict()

            try:
                # Process the item here
                borger = nexus_borgere.get_citizen(item.reference)

                # Finder borgers indsatser
                pathway = nexus_borgere.get_citizen_pathway(borger)
                basket_grant_references = nexus_borgere.get_citizen_pathway_references(
                    pathway
                )
                borgers_indsats_referencer = filter_references(
                    basket_grant_references,
                    path="/Sundhedsfagligt grundforløb/*/Indsatser/basketGrantReference",
                    active_pathways_only=False,
                )

                # Finder borgers forløbsindplacering
                forløbsindplacering_grundforløb = next(
                    (ref for ref in basket_grant_references if ref["name"] == "ÆHF - Forløbsindplacering (Grundforløb)"),
                    None
                )
                forløbsindplacering_raw = (
                    forløbsindplacering_grundforløb["children"][0]["children"][0]["children"][0]
                    if forløbsindplacering_grundforløb
                    else None
                )

                # Pakker forløbsindplacering ud for at få nuværende opgaver. Skipper borger hvis der allerede er oprettet en opgave på forløbsindplacering
                resolved_forløbsindplacering_grundforløb = nexus_borgere.resolve_reference(forløbsindplacering_raw)
                forløbsindplacering_opgaver = nexus_opgaver.get_assignments(resolved_forløbsindplacering_grundforløb)
                if any(opgaver.get("title") == "testopgave fra rpa" for opgaver in forløbsindplacering_opgaver):
                    # Hvis der allerede er oprettet en opgave, så spring denne borger over
                    continue
                
                # Matcher fundne forløbsindplacering med forløbsindplaceringslisten fra Myndighed
                matchende_forløbsindplacering = next(
                    (f for f in forløbsindplacering if f["navn"] == forløbsindplacering_raw["name"]),
                    None
                )

                # Henter borgers kalender
                borger_kalender = nexus_kalender.get_citizen_calendar(borger)
                borger_kalender_begivenheder = nexus_kalender.events(
                    borger_kalender, date.today() + timedelta(days=2), date.today() + timedelta(weeks=26)
                )

                # Gennemgår borgers indsatser og ignorer ikke godkendte indsatser
                for reference in borgers_indsats_referencer:
                    if (
                        reference["name"] not in godkendte_indsatser
                        or reference["workflowState"]["name"] not in godkendte_states
                    ):
                        continue
                    
                    # Pakker indsats ud for at få nuværende opgaver samt id
                    resolved_reference = nexus_borgere.resolve_reference(reference)
                    
                    # Finder id
                    nuværende_bestilling = nexusklient.get(
                        resolved_reference["_links"]["currentOrderedGrant"]["href"]
                    ).json()

                    # Check om der er kalenderbegivenhed for denne indsats
                    matchende_begivenhed = next(
                        (
                            begivenhed
                            for begivenhed in borger_kalender_begivenheder
                            if f"ORDER_GRANT:{nuværende_bestilling['id']}" in begivenhed[
                                "patientGrantIdentifiers"
                            ]
                        ),
                        None,
                    )
                    # Hvis der er en matchende begivenhed, så ignorer denne indsats
                    if matchende_begivenhed:
                        continue

                    # Hvis der ikke er en forløbsindplacering, så sæt matchende_indsats["ansvarlig_organisation"] til "Sygeplejerådgivere fysisk". 
                    # Burde aldrig ramme her
                    if forløbsindplacering_raw is None:
                        matchende_forløbsindplacering = {
                            "ansvarlig_organisation": "Sygeplejerådgivere fysisk"
                        }


                    print(matchende_forløbsindplacering["ansvarlig_organisation"])
                    inaktiver_indsats(borger, resolved_reference)
                    
                #Opret én opgave pr. borger på forløbsindplacering. Ligegyldgt om vi har lukket indsatser eller ej.
                nexus_opgaver.create_assignment(
                    object=resolved_reference,
                    assignment_type="Tværfagligt samarbejde",
                    title="testopgave fra rpa",
                    responsible_organization=matchende_forløbsindplacering["ansvarlig_organisation"],
                    responsible_worker=None,
                    description=None,
                    start_date=date.today(),
                    due_date=date.today()
                )


                print("stop")
            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Fejl ved processering af item: {data}. Fejl: {e}")
                item.fail(str(e))


def inaktiver_indsats(borger: dict, resolved_indsats: dict):
    """
    Inaktiverer indsats for borger.
    param borger: dict: Borgerens data
    param resolved_indsats: dict: Indsatsens data
    """

    transitions = {"Bestilt": "Afslut", "Bevilliget": "Annullér", "Ændret": "Afslut"}

    if resolved_indsats["workflowState"]["name"] not in transitions:
        raise WorkItemError(
            f"Kan ikke afslutte indsats på borger {borger["patientIdentifier"]["identifier"]} med indsats {resolved_indsats["name"]} da den ikke er i en gyldig tilstand."
        )
    
    opdaterings_felter = {}

    if transitions[resolved_indsats["workflowState"]["name"]] == "Afslut":
        opdaterings_felter["billingEndDate"] = datetime.now().astimezone().isoformat()
        opdaterings_felter["basketGrantEndDate"] = datetime.now().astimezone().isoformat()
    else: 
        opdaterings_felter["cancelledDate"] = datetime.now().astimezone().isoformat()

    try:
        # Rediger indsats
        nexus_indsatser.edit_grant(
            grant=resolved_indsats,changes=opdaterings_felter, transition=transitions[resolved_indsats["workflowState"]["name"]]
        )
        # Afregn indsats
        afregningsklient.track_task("Slutlister sygepleje")
    except Exception as e:
        raise WorkItemError(
            f"Fejl ved inaktivering af indsats på borger {borger['patientIdentifier']['identifier']} med indsats {resolved_indsats['name']}: {e}"
        )

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
    nexus_indsatser = GrantsClient(nexus_client=nexusklient)

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
