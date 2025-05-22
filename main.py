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

        
        for borger in borgere:
            try:
                # skip Nancy
                if borger["patientIdentifier"]["identifier"] == "251248-9996":
                    continue

                # skip indlagte borgere
                if borger["patientState"]["name"] == "Indlagt":
                    continue

                cpr = borger["patientIdentifier"]["identifier"]
                workqueue.add_item({}, cpr)
            except Exception as e:
                logger.error(f"Fejl ved tilføjelse af borger {borger}: {e}")


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    for item in workqueue:
        with item:
            lav_opgave_ref = [True]  # Standard: vi vil gerne oprette opgave for hver ny borger
            data = item.get_data_as_dict()

            try:
                borger = nexus_borgere.get_citizen(item.reference)

                # Finder borgers indsatser                
                pathway = nexus_borgere.get_citizen_pathway(borger)
                basket_grant_references = nexus_borgere.get_citizen_pathway_references(pathway)
                borgers_indsats_referencer = filter_references(
                    basket_grant_references,
                    path="/Sundhedsfagligt grundforløb/*/Indsatser/basketGrantReference",
                    active_pathways_only=False,
                )
                # Hvis borger har Kompleks sygepleje (SUL §138) eller Grundlæggende sygepleje (SUL §138) så fortsæt - ellers skip borger
                fundet_sygepleje = next(
                    (
                        ref for ref in borgers_indsats_referencer
                        if (
                            ref["name"] in [
                                "Kompleks sygepleje (SUL §138)",
                                "Grundlæggende sygepleje (SUL §138)"
                            ]
                            and ref["workflowState"]["name"] in godkendte_states
                        )
                    ),
                    None
                )
                if not fundet_sygepleje:
                    continue  # Borger har ikke relevant sygepleje, spring over

                # Finder borgers forløbsindplacering
                forløbsindplacering_grundforløb = next(
                    (ref for ref in basket_grant_references if ref["name"] == "ÆHF - Forløbsindplacering (Grundforløb)"),
                    None
                )

                forløbsindplacering_raw = None

                if forløbsindplacering_grundforløb:
                    second_children = forløbsindplacering_grundforløb["children"][0]["children"]
                    indsats_node = next((child for child in second_children if child.get("name") == "Indsatser"), None)
                    if indsats_node and "children" in indsats_node and indsats_node["children"]:
                        forløbsindplacering_raw = indsats_node["children"][0]
                # forløbsindplacering_raw = (
                #     forløbsindplacering_grundforløb["children"][0]["children"][0]["children"][0]
                #     if forløbsindplacering_grundforløb
                #     else None
                # )

                # Pakker forløbsindplacering ud for at få nuværende opgaver. Skipper borger hvis der allerede er oprettet en opgave på forløbsindplacering
                resolved_forløbsindplacering = nexus_borgere.resolve_reference(forløbsindplacering_raw)
                forløbsindplacering_opgaver = nexus_opgaver.get_assignments(resolved_forløbsindplacering)

                if any(opgaver.get("title") == "Myndighed sygeplejevisitation - Ophør af hjælp" for opgaver in forløbsindplacering_opgaver):
                    continue  # Der findes allerede en opgave, spring borger over
                
                # Matcher fundne forløbsindplacering med forløbsindplaceringslisten fra Myndighed
                matchende_forløbsindplacering = next(
                    (f for f in forløbsindplacering if f["navn"] == forløbsindplacering_raw["name"]),
                    None
                )

                # Henter borgers kalenderbegivenheder
                try:
                    borger_kalender = nexus_kalender.get_citizen_calendar(borger)
                    borger_kalender_begivenheder = nexus_kalender.events(
                        borger_kalender, date.today(), date.today() + timedelta(weeks=26)
                    )
                except Exception as e:
                    raise WorkItemError(
                        f"Fejl ved hentning af kalenderbegivenheder for borger {borger['patientIdentifier']['identifier']}: {e}"
                    )

                # Gennemgår borgers indsatser og lukker hvis relevant – sæt lav_opgave_ref til False hvis begivenhed findes
                vurder_om_indsats_skal_lukkes(
                    borgers_indsats_referencer=borgers_indsats_referencer,
                    godkendte_indsatser=godkendte_indsatser,
                    godkendte_states=godkendte_states,
                    nexus_borgere=nexus_borgere,
                    nexusklient=nexusklient,
                    borger_kalender_begivenheder=borger_kalender_begivenheder,
                    borger=borger,
                    lav_opgave_flag=lav_opgave_ref
                )

                if lav_opgave_ref[0]:  # Kun hvis vi ikke fandt en begivenhed, skal vi oprette opgaven
                    nexus_opgaver.create_assignment(
                        object=resolved_forløbsindplacering,
                        assignment_type="Myndighed sygeplejevisitation - Ophør af hjælp",
                        title="Myndighed sygeplejevisitation - Ophør af hjælp",
                        responsible_organization=matchende_forløbsindplacering["ansvarlig_organisation"],
                        responsible_worker=None,
                        description=None,
                        start_date=date.today(),
                        due_date=date.today()
                    )

            except WorkItemError as e:
                logger.error(f"Fejl ved processering af item: {data}. Fejl: {e}")
                item.fail(str(e))


def vurder_om_indsats_skal_lukkes(
    borgers_indsats_referencer,
    godkendte_indsatser,
    godkendte_states,
    nexus_borgere,
    nexusklient,
    borger_kalender_begivenheder,
    borger,
    lav_opgave_flag  # Nyt argument som vi kan ændre inde i funktionen
):
    """
    Gennemgår en liste af indsatsreferencer for en borger og inaktiverer de indsatser,
    som er godkendte og ikke har en tilknyttet kalenderbegivenhed.
    Hvis der findes en begivenhed for en indsats, sættes lav_opgave_flag til False.

    En indsats bliver kun inaktiveret, hvis:
    - Navnet findes i de godkendte indsatser (lowercase).
    - WorkflowState er blandt de godkendte states.
    - Der ikke findes en kalenderbegivenhed, som matcher indsatsen.

    Argumenter:
        borgers_indsats_referencer (list): Liste af indsatsreferencer for borgeren.
        godkendte_indsatser (set eller list): Navne på godkendte indsatser (lowercase).
        godkendte_states (set eller list): Navne på godkendte workflow states.
        nexus_borgere: Nexus klient til at resolve referencer.
        nexusklient: Nexus HTTP-klient til opslag af currentOrderedGrant.
        borger_kalender_begivenheder (list): Liste over borgerens kalenderbegivenheder.
        borger: Borgerobjekt, der skal bruges ved inaktivering.
    """
    for reference in borgers_indsats_referencer:
        # Tjek om indsatsen er godkendt, forventet state eller om den er ældre eller ikke ændret inden for 14 dage
        i_dag_minus_14_dage = datetime.now().astimezone() - timedelta(days=14)
        reference_dag = datetime.strptime(reference["date"], "%Y-%m-%dT%H:%M:%S.%f%z")
        if (
            reference["name"].lower() not in godkendte_indsatser
            or reference["workflowState"]["name"] not in godkendte_states
        ):
            continue

        resolved_reference = nexus_borgere.resolve_reference(reference)
        # Hvis det er en akutindsats, så har den ikke en bestilling
        if reference["workflowState"]["name"] != "Oprettet (Akut)":
            try:
                # Hent nuværende bestilling
                nuværende_bestilling = nexusklient.get(
                    resolved_reference["_links"]["currentOrderedGrant"]["href"]
                ).json()
            except Exception as e:
                raise WorkItemError(
                    f"Fejl ved hentning af nuværende bestilling for indsats {resolved_reference['name']}: {e}"
                )   
        else: # Akutindsats har Id direkte liggende.
            nuværende_bestilling = {}
            nuværende_bestilling["id"] = resolved_reference["currentOrderGrantId"]


        # Find ud af om der allerede findes en kalenderbegivenhed for denne indsats. BASKET_GRANT er for akutindsatser
        matchende_begivenhed = next(
            (
                begivenhed
                for begivenhed in borger_kalender_begivenheder
                if isinstance(begivenhed.get("patientGrantIdentifiers"), list) and (
                    f"ORDER_GRANT:{nuværende_bestilling['id']}" in begivenhed["patientGrantIdentifiers"] or
                    f"BASKET_GRANT:{nuværende_bestilling['id']}" in begivenhed["patientGrantIdentifiers"]
                )
            ),
            None,
        )

        if matchende_begivenhed:
            lav_opgave_flag[0] = False  # Der findes en begivenhed – vi skal ikke oprette ny opgave
            continue

        if reference_dag > i_dag_minus_14_dage:
            lav_opgave_flag[0] = False # Der findes en indsats, der er bestilt eller ændret inden for 14 dage fra d.d. – vi skal ikke oprette ny opgave
            continue

        if reference["workflowState"]["name"] == "Oprettet (Akut)":
            continue # Akutindsats kan ikke afsluttes
            

        inaktiver_indsats(borger, resolved_reference)

def inaktiver_indsats(borger: dict, resolved_indsats: dict):
    """
    Inaktiverer indsats for borger.
    param borger: dict: Borgerens data
    param resolved_indsats: dict: Indsatsens data
    """

    transitions = {"Bestilt": "Afslut", "Ændret": "Afslut"}

    if resolved_indsats["workflowState"]["name"] not in transitions:
        raise WorkItemError(
            f"Kan ikke afslutte indsats på borger {borger["patientIdentifier"]["identifier"]} med indsats {resolved_indsats["name"]} da den ikke er i en gyldig tilstand."
        )
    
    opdaterings_felter = {}

    if transitions[resolved_indsats["workflowState"]["name"]] == "Afslut":
        opdaterings_felter["billingEndDate"] = datetime.now().astimezone().isoformat()
        opdaterings_felter["basketGrantEndDate"] = datetime.now().astimezone().isoformat()


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
