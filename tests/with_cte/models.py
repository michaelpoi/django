from django.db import models


class Distributor(models.Model):
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name


class Film(models.Model):
    title = models.CharField(max_length=100)
    duration = models.DurationField()
    release_year = models.DateField()
    distributor = models.ForeignKey(Distributor, on_delete=models.CASCADE)

    def __str__(self):
        return self.title
