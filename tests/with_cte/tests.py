from django.core.exceptions import FieldError
from django.db import connection
from django.db.models import (
    CharField,
    Model,
    Avg, Count, DecimalField, DurationField, F, FloatField, Func, IntegerField,
    Max, Min, Sum, Value
)
from django.db.models.expressions import Case, Exists, OuterRef, Subquery, With, When
from django.db.models.functions import Length
from django.test import TestCase
from django.test.testcases import skipUnlessDBFeature
from django.test.utils import Approximate, CaptureQueriesContext
from django.utils.timezone import datetime, timedelta

from .models import Distributor, Film, Dummy


class year(datetime):
    def __new__(cls, year):
        return super().__new__(cls, year, 1, 1)

class CTETestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        d1 = Distributor.objects.create(name='20th Century Fox')
        d2 = Distributor.objects.create(name='Metro-Goldwyn-Mayer')
        d3 = Distributor.objects.create(name='Paramount Pictures')
        d4 = Distributor.objects.create(name='Universal')

        Film.objects.create(title='Airplane Sierra', duration=timedelta(minutes=62), release_year=year(2006), distributor=d1)
        Film.objects.create(title='Blackout Private', duration=timedelta(minutes=85), release_year=year(2009), distributor=d1)
        Film.objects.create(title='Bugsy Song', duration=timedelta(minutes=119), release_year=year(2006), distributor=d2)
        Film.objects.create(title='Christmas Moonshine', duration=timedelta(minutes=150), release_year=year(2005), distributor=d4)
        Film.objects.create(title='Destiny Saturday', duration=timedelta(minutes=56), release_year=year(2007), distributor=d3)
        Film.objects.create(title='Flamingos Connecticut', duration=timedelta(minutes=80), release_year=year(2003), distributor=d2)
        Film.objects.create(title='Gentlemen Stage', duration=timedelta(minutes=55), release_year=year(2011), distributor=d1)
        Film.objects.create(title='Hollywood Anonymous', duration=timedelta(minutes=69), release_year=year(2009), distributor=d3)
        Film.objects.create(title='Impossible Prejudice', duration=timedelta(minutes=103), release_year=year(2017), distributor=d2)
        Film.objects.create(title='Jason Trap', duration=timedelta(minutes=130), release_year=year(2013), distributor=d4)
        Film.objects.create(title='King Evolution', duration=timedelta(minutes=184), release_year=year(2012), distributor=d2)
        Film.objects.create(title='Lebowski Soldiers', duration=timedelta(minutes=59), release_year=year(2018), distributor=d1)
        Film.objects.create(title='Meet Chocolate', duration=timedelta(minutes=80), release_year=year(2008), distributor=d3)
        Film.objects.create(title='Punk Divorce', duration=timedelta(minutes=101), release_year=year(2005), distributor=d4)
        Film.objects.create(title='Rock Instinct', duration=timedelta(minutes=73), release_year=year(2010), distributor=d1)

    def t_est_case_when(self):
        """
        SELECT film_id, title, (CASE
            WHEN length < 60 THEN 'short'
            WHEN length >= 60 AND length <= 100 THEN 'regular'
            WHEN length > 100 THEN 'long'
        END) length FROM film
            WHERE length='short'
        """
        print('=== Test Case When ===')
        qs = Film.objects.annotate(
            length=Case(
                When(duration__lt=timedelta(minutes=60), then=Value('short')),
                When(duration__gte=timedelta(minutes=60), duration__lte=timedelta(minutes=100), then=Value('regular')),
                When(duration__gt=timedelta(minutes=100), then=Value('long')),
                output_field=CharField(),
            )
        ).values_list('title', 'length').filter(length='short')
        self.assertIs(qs.count(), 3)
        sql, params = qs.query.sql_with_params()
        sql = sql % params
        print(sql)

    def t_est_related_query(self):
        """
        SELECT film_id, title, (CASE
            WHEN length < 60 THEN 'short'
            WHEN length >= 60 AND length <= 100 THEN 'regular'
            WHEN length > 100 THEN 'long'
        END) length FROM film
                    INNER JOIN distributor ON (film.distributor_id=distributor.id)
            WHERE distributor WHERE distributor.name='20th Century Fox'
        """
        print('=== Test Related Query ===')
        qs = Film.objects.annotate(
            length=Case(
                When(duration__lt=timedelta(minutes=60), then=Value('short')),
                When(duration__gte=timedelta(minutes=60), duration__lte=timedelta(minutes=100), then=Value('regular')),
                When(duration__gt=timedelta(minutes=100), then=Value('long')),
                output_field=CharField(),
            )
        ).values_list('title', 'length')
        qs = qs.filter(distributor__name='20th Century Fox')
        self.assertIs(qs.count(), 15)
        sql, params = qs.query.sql_with_params()
        sql = sql % params
        print(sql)

    def test_non_recursive_self_referring(self):
        """
        WITH cte_film AS (
            SELECT film_id, title_, (CASE
                WHEN length < 60 THEN "short"
                WHEN length >= 60 AND length <= 100 THEN "regular"
                WHEN length > 100 THEN "long"
                ELSE "unclassified"
            END) length, title AS title_, Length(title) AS titlen FROM film
        )
        SELECT id, title FROM cte_film;
        """
        inner_qs = Film.objects.annotate(
            length=Case(
                When(duration__lt=timedelta(minutes=60), then=Value('short')),
                When(duration__gte=timedelta(minutes=60), duration__lte=timedelta(minutes=100), then=Value('regular')),
                When(duration__gt=timedelta(minutes=100), then=Value('long')),
                default=Value('unclassified'),
                output_field=CharField(),
            ),
            title_=F('title'),
            titlen=Length('title'),
        )

        cte_film = With(inner_qs)
        CTEFilm = cte_film.get_common_expression_model('cte_film')
        outer_qs = CTEFilm.objects.annotate(cte_film=cte_film)
        sql, params = outer_qs.query.sql_with_params()
        self.assertEqual(sql % params, 'WITH cte_film AS (SELECT "with_cte_film"."id", "with_cte_film"."title", "with_cte_film"."duration", "with_cte_film"."release_year", "with_cte_film"."distributor_id", CASE WHEN "with_cte_film"."duration" < 3600000000 THEN short WHEN ("with_cte_film"."duration" >= 3600000000 AND "with_cte_film"."duration" <= 6000000000) THEN regular WHEN "with_cte_film"."duration" > 6000000000 THEN long ELSE unclassified END AS "length", "with_cte_film"."title" AS "title_", LENGTH("with_cte_film"."title") AS "titlen" FROM "with_cte_film") SELECT "cte_film"."id", "cte_film"."length", "cte_film"."title_", "cte_film"."titlen" FROM "cte_film"')
        self.assertIs(outer_qs.count(), 15)
        qs = outer_qs.filter(length='regular')
        self.assertIs(qs.count(), 6)
        qs = outer_qs.filter(titlen=15)
        self.assertIs(qs.count(), 2)

    def test_non_recursive_with_referrer(self):
        """
        WITH cte_film AS (
            SELECT film_id, title_, (CASE
                WHEN length < 60 THEN "short"
                WHEN length >= 60 AND length <= 100 THEN "regular"
                WHEN length > 100 THEN "long"
                ELSE "unclassified"
            END) length, title AS title_, Length(title) AS titlen FROM film
        )
        SELECT id, name FROM distributor;
        """
        print('=== Test Non-Recursive CTE with filter ===')
        inner_qs = Film.objects.annotate(
            length=Case(
                When(duration__lt=timedelta(minutes=60), then=Value('short')),
                When(duration__gte=timedelta(minutes=60), duration__lte=timedelta(minutes=100), then=Value('regular')),
                When(duration__gt=timedelta(minutes=100), then=Value('long')),
                default=Value('unclassified'),
                output_field=CharField(),
            ),
        )
        outer_qs = Distributor.objects.annotate(cte_film=With(inner_qs))
        self.assertIs(outer_qs.count(), 4)
        sql, params = outer_qs.query.sql_with_params()
        print(sql % params)
        outer_qs = outer_qs.filter(cte_film__length='short')
