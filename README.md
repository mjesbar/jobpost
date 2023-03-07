# JobPost - Project

## Review
This project collects information about job posts published in 'Computrabajo' and classiffy 3 tables in different aspects.
Those tables organize and struct many data relevant for the one whose purpose is to search for a job. The information structured
and stored is wheater require such programming language, english speaking, or domains certain software.

## Project Diagram
![ProjectaDiagram](./diagrams/diagram4.png "ProjectaDiagram")

## Running the Project
the project is only reproducible if you meet the following requirement in your premise.<br>
<br>
- An Amazon Web Services Account, Using a full-permissions account. (Not root, but admin)
- python==3.9
- boto3==1.26.79
- botocore==1.29.79
- awscli==2.9.11
- selenium==4.8.0
- chromium==110.0.5481.177
- pandas==1.5.2
- pyarrow==11.0.0 (required to handle parquet)
- numpy==1.24.1

* to create all the resources necessaries for the project run:<br>
<br>
``chmod 744 ./deployment``<br>
``./deployment``<br>


