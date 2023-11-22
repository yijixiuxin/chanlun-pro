from django import template

register = template.Library()


@register.filter('hash')
def hash(dic, key):
    return dic[key]
