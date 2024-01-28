

from sqlalchemy import create_engine  , text
import os

db_connection_string = os.environ['DB_CONNECTION_STRING']

engine = create_engine(db_connection_string, connect_args={
        "ssl": {
            "ssl_ca": "/etc/ssl/cert.pem"
        }
  
})

def load_jobs_from_db():
  with engine.connect() as conn:
    result = conn.execute(text("select * from job"))
    job = []
    for row in result.all():
      job.append(row._asdict())
    return job  


def load_job_from_db(id):
  with engine.connect() as conn:
    result = conn.execute(text("select * from job where id = :val"),{"val":id})
    rows = result.all()
    if len(rows) == 0:
      return None
    else:
      return rows[0]._asdict()


def add_application_to_db(job_id, data):
  with engine.connect() as conn:
      query = text("insert into applications (job_id, full_name, email,  education) values (:job_id, :full_name, :email,  :education)")
      conn.execute(query, {"job_id": job_id, "full_name": data['full_name'], "email": data['email'], "education": data['education']})
