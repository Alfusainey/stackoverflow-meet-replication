import json
import logging
import os
from typing import Dict

import openai
import pandas as pd
import requests
from sqlalchemy import create_engine

from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential
)


@retry(wait=wait_random_exponential(min=1, max=10), stop=stop_after_attempt(6))
def classify_paper(api_key, paper_abstract, model='gpt-4o') -> tuple:
    openai.api_key = api_key
    response = openai.chat.completions.create(  # Updated method
        model=model,  # Replace with "gpt-4-turbo" if preferred
        messages=[
            {"role": "system",
             "content": "You are an expert in analyzing academic research focused on the security or correctness (e.g., buggy or faulty) of code snippets on Stack Overflow. Do not consider studies that focus on general security discussions or questions about security, unless they specifically analyze the security of the code snippets themselves, or identify and address issues such as bugs or faults in the code snippets."},
            {"role": "user",
             "content": f'Does this paper study the security of code snippets on Stack Overflow or identify and address issues such as bugs or faults in the code snippets? Provide a "Yes" or "No" and a brief justification in the following JSON format: {{"SecurityRelevant"": "True" or "False", "Justification": "brief justification"}}: \n\n{paper_abstract}'}
        ],
        temperature=0.2,
        response_format={  # I needed to define the response format to get the JSON object when using the GTP-4o model
            "type": "json_object"
        }
    )

    content = response.choices[0].message.content  # Access response text
    content = json.loads(content)  # Convert to JSON
    result = True if content.get("SecurityRelevant") == 'True' else False, content.get("Justification")
    return result


def is_dblp_indexed(venue_name: str) -> bool:
    """Check if the venue is indexed in DBLP using GPT-4o."""
    openai.api_key = get_api_key()
    model = 'gpt-4o'
    response = openai.chat.completions.create(  # Updated method
        model=model,  # Replace with "gpt-4-turbo" if preferred
        messages=[
            {"role": "system",
             "content": "You are an expert in determining whether an academic venue is DBLP-indexed or not. A venue is DBLP-indexed if it is included in the DBLP database."},
            {"role": "user",
             "content": f'Is this venue DBLP-indexed? Provide only a "Yes" or "No": \n\n{venue_name}'}
        ],
        temperature=0.2
    )
    answer = response.choices[0].message.content  # Access response text
    return answer == "Yes"


@retry(wait=wait_random_exponential(min=1, max=10), stop=stop_after_attempt(6))
def crawl(url: str) -> requests.Response:
    """Fetch content from the specified URL."""
    response = requests.get(url)
    response.raise_for_status()  # Check for HTTP errors
    return response


def get_api_key():
    return os.environ["OPENAI_API_KEY"]


def is_none_or_empty(value):
    if isinstance(value, str):
        return value is None or value.strip() == ''
    return value is None


def title_too_long(title):
    return len(title) > 250


def get_venues():
    return {
        ("uss", "conference"): "USENIX Security Symposum",
        ("acsac", "conference"): "Annual Computer Security Applications Conference (ACSAC)",
        ("ccs", "conference"): "ACM Conference on Computer and Communications Security (CCS)",
        ("sp", "conference"): "IEEE Symposium on Security and Privacy (SP)",
        ("ndss", "conference"): "Network and Distributed System Security Symposium (NDSS)",
        ("dimva", "conference"): "Detection of Intrusions and Malware & Vulnerability Assessment (DIMVA)",
        ("chi", "conference"): "Conference on Human Factors in Computing Systems (CHI)",
        ("cscw", "conference"): "ACM Conference on Computer-Supported Cooperative Work (CSCW)",
        ("uist", "conference"): "ACM Symposium on User Interface Software and Technology (UIST)",
        ("sigsoft", "conference"): "ACM SIGSOFT Symposium on the Foundations of Software Engineering (FSE)",
        ("icse", "conference"): "International Conference on Software Engineering (ICSE)",
        ("kbse", "conference"): "International Conference on Automated Software Engineering (ASE)",
        ("icsm", "conference"): "IEEE International Conference on Software Maintenance (ICSM)", # acronym also applies to IEEE International Conference on Software Maintenance and Evolution (ICSME)
        ("issre", "conference"): "IEEE International Symposium on Software Reliability Engineering (ISSRE)",
        ("icst", "conference"): "IEEE International Conference on Software Testing, Verification and Validation (ICST)",
        ("msr", "conference"): "Mining Software Repositories (MSR)",
        ("esem", "conference"): "Empirical Software Engineering and Measurement (ESEM)",
        ("saner", "conference"): "IEEE International Conference on Software Analysis, Evolution, and Reengineering (SANER)",
        ("esec", "conference"): "European Software Engineering Conference (ESEC)",
        ("wcre", "conference"): "IEEE Working Conference on Reverse Engineering (WCRE)",
        ("issta", "conference"): "International Symposium on Software Testing and Analysis (ISSTA)",
        ("fase", "conference"): "International Conference on Fundamental Approaches to Software Engineering (FASE)",
        ("iwpc", "conference"): "IEEE International Conference on Program Comprehension (ICPC)",
        ("www", "conference"): "International World Wide Web Conference (WWW)",
        ("asiaccs", "conference"): "ACM Asia Conference on Computer and Communications Security (ASIACCS)",
        ("eurosp", "conference"): "IEEE European Symposium on Security and Privacy (EuroS&P)",
        ("tse", "journal"): "IEEE Transactions on Software Engineering (TSE)",
        ("ese", "journal"): "Empirical Software Engineering (ESE)",
        ("tosem", "journal"): "ACM Transactions on Software Engineering and Methodology (TOSEM)",
    }


def get_non_dblp_indexed_venues():
    return ['']


def get_search_keywords():
    return ['stackoverflow', 'stack overflow', 'crowd knowledge', 'crowdsource knowledge', 'crowd-source knowledge',
            'Q&A websites', 'Q&A sites', 'social Q&A websites', 'online Q&A communities', 'Q&A Forums',
            'community question answering', 'knowledge sharing', 'knowledge-sharing', 'crowdsourcing',
            'Online Code Snippets']


def get_logger(name: str = 'app.log'):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s -  %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    file_handler = logging.FileHandler(name, mode='a')
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def get_db_engine(db_user, db_password, database, db_host='127.0.0.1'):
    return create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{database}")


def get_venue_type(paper_dict: Dict):

    if 'publicationTypes' in paper_dict and paper_dict.get('publicationTypes'):
        publication_types = [publication_type.lower() for publication_type in paper_dict.get('publicationTypes') if publication_type]
        if 'conference' in publication_types:
            return 'conference'.upper()
        elif 'JournalArticle'.lower() in publication_types:
            return 'journal'.upper()
    return None


def get_venue_name(paper_dict: Dict):
    if 'journal' in paper_dict:
        if 'name' in paper_dict['journal']:
            return paper_dict['journal']['name']
    return None


def get_abstracts():
    a1 = """Online programming discussion platforms such as Stack Overflow serve as a rich source of information 
    for software developers. Available information include vibrant discussions and oftentimes ready-to-use code 
    snippets. Previous research identified Stack Overflow as one of the most important information sources developers 
    rely on. Anecdotes report that software developers copy and paste code snippets from those information sources 
    for convenience reasons. Such behavior results in a constant flow of community-provided code snippets into 
    production software. To date, the impact of this behaviour on code security is unknown. We answer this highly 
    important question by quantifying the proliferation of security-related code snippets from Stack Overflow in 
    Android applications available on Google Play. Access to the rich source of information available on Stack 
    Overflow including ready-to-use code snippets provides huge benefits for software developers. However, 
    when it comes to code security there are some caveats to bear in mind: Due to the complex nature of code 
    security, it is very difficult to provide ready-to-use and secure solutions for every problem. Hence, integrating 
    a security-related code snippet from Stack Overflow into production software requires caution and expertise. 
    Unsurprisingly, we observed insecure code snippets being copied into Android applications millions of users 
    install from Google Play every day. To quantitatively evaluate the extent of this observation, we scanned Stack 
    Overflow for code snippets and evaluated their security score using a stochastic gradient descent classifier. In 
    order to identify code reuse in Android applications, we applied state-of-the-art static analysis. Our results 
    are alarming: 15.4% of the 1.3 million Android applications we analyzed, contained security-related code snippets 
    from Stack Overflow. Out of these 97.9% contain at least one insecure code snippet. 
    """

    a2 = """The Java platform and its third-party libraries provide useful features to facilitate secure coding. 
    However, misusing them can cost developers time and effort, as well as introduce security vulnerabilities in 
    software. We conducted an empirical study on StackOverflow posts, aiming to understand developers' concerns on 
    Java secure coding, their programming obstacles, and insecure coding practices. We observed a wide adoption of 
    the authentication and authorization features provided by Spring Security—a third-party framework designed to 
    secure enterprise applications. We found that programming challenges are usually related to APIs or libraries, 
    including the complicated cross-language data handling of cryptography APIs, and the complex Java-based or 
    XML-based approaches to configure Spring Security. In addition, we reported multiple security vulnerabilities in 
    the suggested code of accepted answers on the StackOverflow forum. The vulnerabilities included disabling the 
    default protection against Cross-Site Request Forgery (CSRF) attacks, breaking SSL/TLS security through bypassing 
    certificate validation, and using insecure cryptographic hash functions. Our findings reveal the insufficiency of 
    secure coding assistance and documentation, as well as the huge gap between security theory and coding practices. 

    """

    a3 = """"Programming code snippets readily available on platforms such as StackOverflow are undoubtedly useful 
    for software engineers. Unfortunately, these code snippets might contain issues such as deprecated, misused, 
    or even buggy code. These issues could pass unattended, if developers do not have adequate knowledge, time, 
    or tool support to catch them. In this work we expand the understanding of such issues (or the so called 
    ""violations"") hidden in code snippets written in JavaScript, the programming language with the highest number 
    of questions on StackOverflow. To characterize the violations, we extracted 336k code snippets from answers to 
    JavaScript questions on StackOverflow and statically analyzed them using ESLinter, a JavaScript linter. We 
    discovered that there is no single JavaScript code snippet without a rule violation. On average, our studied code 
    snippets have 11 violations, but we found instances of more than 200 violations. In particular, rules related to 
    stylistic issues are by far the most violated ones (82.9% of the violations pertain to this category). Possible 
    errors, which developers might be more interested in, represent only 0.1% of the violations. Finally, we found a 
    small fraction of code snippets flagged with possible errors being reused on actual GitHub software projects. 
    Indeed, one single code snippet with possible errors was reused 1,261 times." 

    """

    a4 = """Stack Overflow is the most popular discussion platform for software developers. Recent research found 
    a large amount of insecure encryption code in production systems that has been inspired by examples given on 
    Stack Overflow. By copying and pasting functional code, developers introduced exploitable software 
    vulnerabilities into security-sensitive high-profile applications installed by millions of users every day. 
    Proposed mitigations of this problem suffer from usability flaws and push developers to continue shopping for 
    code examples on Stack Overflow once again. This points us to fighting the proliferation of insecure code 
    directly at the root before it even reaches the clipboard. By viewing Stack Overflow as a market, implementation 
    of cryptography becomes a decision-making problem: i. e. how to simplify the selection of helpful and secure 
    examples. We focus on supporting software developers in making better decisions by applying nudges, 
    a concept borrowed from behavioral science. This approach is motivated by one of our key findings: for 99.37% of 
    insecure code examples on Stack Overflow, similar alternatives are available that serve the same use case and 
    provide strong cryptography. Our system design is based on several nudges that are controlled by a deep neural 
    network. It learns a representation for cryptographic API usage patterns and classification of their security, 
    achieving average AUC-ROC of 0.992. With a user study we demonstrate that nudge-based security advice 
    significantly helps tackling the most popular and error-prone cryptographic use cases in Android. """

    a5 = """Stack Overflow is noteworthy in the value it provides to the practitioner community. However, 
    concerns have also been expressed around the quality of content that is included on this website. Researchers 
    have thus studied Stack Overflow code snippet quality, with particular focus on security issues. However, 
    there has been less effort aimed at providing concrete solutions and demonstrations of the debilitating effects (
    vulnerabilities) of Stack Overflow security issues. In this pilot study, we bridge this gap by using inductive 
    analyses to study the scale of security faults on Stack Overflow, and contributors' awareness of these faults and 
    their fixes. We also examine the potential debilitating effects of Stack Overflow code snippets security faults 
    and mechanisms for cataloguing security faults for practitioners. Our outcomes reveal that most Stack Overflow 
    code snippets do not possess security faults. However, some code snippets with faults are accepted by the 
    community. We note that the security faults observed in Stack Overflow code snippets are likely to result in 
    various levels of security breaches and ensuing consequences. Thus, our attempt at providing a preliminary 
    catalogue could be useful and encouraging for the community to further develop. While this is an initial step, 
    ongoing efforts aimed at investigating and enhancing the code on online portals are noteworthy for helping the 
    community to deliver secure software. """

    a6 = """Existing research has shown that developers will use StackOverflow to answer programming questions: 
    but what draws them to one particular answer over any other? The choice of answer they select can mean the 
    difference between a secure application and insecure one, as the quality of supposedly secure answers can vary. 
    Prior work has studied people posting on Stack Overflow—a two-way communication between the original poster and 
    the Stack Overflow community. Instead, we study the situation of one-way communication, where people only read a 
    Stack Overflow thread without being actively involved in it, sometimes long after a thread has closed. We report 
    on a mixed-method study including a controlled between-groups experiment and qualitative analysis of 
    participants’ rationale (N=1188), investigating whether explanation detail, answer scoring, accepted answer 
    marks, as well as the security of the code snippet itself affect the answers participants accept. Our findings 
    indicate that explanation detail affects what answers participants reading a thread select (p<0.01), while answer 
    score and acceptance do not (p>0.05)—the inverse of what research has shown for those asking and answering 
    questions. The qualitative analysis of participants’ rationale further explains how several cognitive biases 
    underpin these findings. Correspondence bias, in particular, plays an important role in instilling readers with a 
    false sense of confidence in an answer through the way it looks , regardless of whether it works, is secure, 
    or if the community agrees with it. As a result, we argue that StackOverflow's use as a knowledge base by people 
    not actively involved in threads—when there is only one-way-communication—may inadvertently contribute to the 
    spread of insecure code, as the community's voting mechanisms hold little power to deter them from answers. """

    a7 = """Online code clones are code fragments that are copied from software projects or online sources to Stack 
    Overflow as examples. Due to an absence of a checking mechanism after the code has been copied to Stack Overflow, 
    they can become toxic code snippets, e.g., they suffer from being outdated or violating the original software 
    license. We present a study of online code clones on Stack Overflow and their toxicity by incorporating two 
    developer surveys and a large-scale code clone detection. A survey of 201 high-reputation Stack Overflow 
    answerers (33 percent response rate) showed that 131 participants (65 percent) have ever been notified of 
    outdated code and 26 of them (20 percent) rarely or never fix the code. 138 answerers (69 percent) never check 
    for licensing conflicts between their copied code snippets and Stack Overflow's CC BY-SA 3.0. A survey of 87 
    Stack Overflow visitors shows that they experienced several issues from Stack Overflow answers: mismatched 
    solutions, outdated solutions, incorrect solutions, and buggy code. 85 percent of them are not aware of CC BY-SA 
    3.0 license enforced by Stack Overflow, and 66 percent never check for license conflicts when reusing code 
    snippets. Our clone detection found online clone pairs between 72,365 Java code snippets on Stack Overflow and 
    111 open source projects in the curated Qualitas corpus. We analysed 2,289 non-trivial online clone candidates. 
    Our investigation revealed strong evidence that 153 clones have been copied from a Qualitas project to Stack 
    Overflow. We found 100 of them (66 percent) to be outdated, of which 10 were buggy and harmful for reuse. 
    Furthermore, we found 214 code snippets that could potentially violate the license of their original software and 
    appear 7,112 times in 2,427 GitHub projects. """

    return [a1, a2, a3, a4, a5, a6, a7]


def pass_by_reference(df: pd.DataFrame):
    for index, row in df.iterrows():
        is_relevant, justification = classify_paper(get_api_key(), row['Abstract'])
        df.loc[index, 'IsRelevant'] = is_relevant
        df.loc[index, 'Justification'] = justification
    # print(df)


def test_pass_by_reference():
    abstracts = get_abstracts()
    df = pd.DataFrame(
        {'Title': ['title 1', 'title 2', 'title 2'], 'Abstract': [abstracts[0], abstracts[1], abstracts[2]]})
    pass_by_reference(df)
    print(df)


if __name__ == '__main__':
    test_pass_by_reference()
    # print
