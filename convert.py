# encoding: utf-8

from pymongo import MongoClient
from peewee import SqliteDatabase, Model, CharField, TextField, DateField, ForeignKeyField
from dateutil.parser import parse

db = SqliteDatabase('question.db')

class WrittenQuestion(Model):
    departement_fr = CharField()
    departement_nl = CharField()
    status_fr = CharField()
    status_nl = CharField()
    publication_question_pdf_url = CharField()
    publication_reponse_pdf_url = CharField()
    question_fr = TextField()
    question_nl = TextField()
    answer_fr = TextField()
    answer_nl = TextField()
    publication_question = CharField()
    publication_reponse = CharField()
    sub_departement_fr = CharField()
    sub_departement_nl = CharField()
    lachambre_id = CharField()
    language = CharField()
    url = CharField()
    title_fr = CharField()
    title_nl = CharField()
    delay_date = DateField()
    publication_date = DateField()
    deposition_date = DateField()

    class Meta:
        database = db


class Tag(Model):
    tag = CharField()
    kind = CharField()

    class Meta:
        database = db


class QuestionTag(Model):
    question = ForeignKeyField(WrittenQuestion)
    tag = ForeignKeyField(Tag)

    class Meta:
        database = db


if __name__ == '__main__':
    import os, sys
    os.system("rm question.db")
    WrittenQuestion.create_table()
    Tag.create_table()
    QuestionTag.create_table()
    client = MongoClient()
    a = 0
    total = client.test.writtenquestions.find().count()
    with db.transaction():
        for question in client.test.writtenquestions.find():
            a += 1
            sys.stdout.write("%s/%s\r" % (a, total))
            sys.stdout.flush()
            written_question = WrittenQuestion.create(
                departement_fr=question["departement"]["fr"],
                departement_nl=question["departement"]["nl"],
                status_fr=question["status"]["fr"],
                status_nl=question["status"]["nl"],
                publication_question_pdf_url=question["publication_question_pdf_url"],
                publication_reponse_pdf_url=question["publication_reponse_pdf_url"],
                question_fr=question["question"]["fr"],
                question_nl=question["question"]["nl"],
                answer_fr=question["answer"]["fr"],
                answer_nl=question["answer"]["nl"],
                publication_question=question["publication_question"],
                publication_reponse=question["publication_reponse"],
                sub_departement_fr=question["sub_departement"]["fr"],
                sub_departement_nl=question["sub_departement"]["nl"],
                lachambre_id=question["lachambre_id"],
                language=question["language"],
                url=question["url"],
                title_fr=question["title"]["fr"],
                title_nl=question["title"]["nl"],
                delay_date=parse(question["delay_date"]),
                publication_date=parse(question["publication_date"].split(",")[0]),
                deposition_date=parse(question["deposition_date"])
            )
            for keyword in question["keywords"]:
                tag = Tag.select().where((Tag.tag == keyword) & (Tag.kind == "keyword"))
                if not tag.count():
                    tag = Tag.create(tag=keyword, kind="keyword")
                else:
                    tag = tag[0]
                QuestionTag.create(question=written_question.id, tag=tag.id)

    sys.stdout.write("\n")
