#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -----------------
# Реализуйте функцию best_hand, которая принимает на вход
# покерную "руку" (hand) из 7ми карт и возвращает лучшую
# (относительно значения, возвращаемого hand_rank)
# "руку" из 5ти карт. У каждой карты есть масть(suit) и
# ранг(rank)
# Масти: трефы(clubs, C), пики(spades, S), червы(hearts, H), бубны(diamonds, D)
# Ранги: 2, 3, 4, 5, 6, 7, 8, 9, 10 (ten, T), валет (jack, J), дама (queen, Q), король (king, K), туз (ace, A)
# Например: AS - туз пик (ace of spades), TH - дестяка черв (ten of hearts), 3C - тройка треф (three of clubs)

# Задание со *
# Реализуйте функцию best_wild_hand, которая принимает на вход
# покерную "руку" (hand) из 7ми карт и возвращает лучшую
# (относительно значения, возвращаемого hand_rank)
# "руку" из 5ти карт. Кроме прочего в данном варианте "рука"
# может включать джокера. Джокеры могут заменить карту любой
# масти и ранга того же цвета. Черный джокер '?B' может быть
# использован в качестве треф или пик любого ранга, красный
# джокер '?R' - в качестве черв и бубен люього ранга.

# Одна функция уже реализована, сигнатуры и описания других даны.
# Вам наверняка пригодится itertools
# Можно свободно определять свои функции и т.п.
# -----------------


import itertools


def hand_rank(hand):
    """Возвращает значение определяющее ранг 'руки'"""
    ranks = card_ranks(hand)
    if straight(ranks) and flush(hand):
        return (8, max(ranks))
    elif kind(4, ranks):
        return (7, kind(4, ranks), kind(1, ranks))
    elif kind(3, ranks) and kind(2, ranks):
        return (6, kind(3, ranks), kind(2, ranks))
    elif flush(hand):
        return (5, ranks)
    elif straight(ranks):
        return (4, max(ranks))
    elif kind(3, ranks):
        return (3, kind(3, ranks), ranks)
    elif two_pair(ranks):
        return (2, two_pair(ranks), ranks)
    elif kind(2, ranks):
        return (1, kind(2, ranks), ranks)
    else:
        return (0, ranks)


def card_ranks(hand):
    """Возвращает список рангов, отсортированный от большего к меньшему"""
    return sorted(['23456789TJQKA'.index(r) for r, s in hand], reverse=True)


def flush(hand):
    """Возвращает True, если все карты одной масти"""
    suits = set(s for r, s in hand)
    return len(suits) == 1


def straight(ranks):
    """Возвращает True, если отсортированные ранги формируют последовательность 5ти,
    где у 5ти карт ранги идут по порядку (стрит)"""
    ranks = set(ranks)
    return len(ranks) == 5 and max(ranks) - min(ranks) == 4


def kind(n, ranks):
    """Возвращает первый ранг, который n раз встречается в данной руке.
    Возвращает None, если ничего не найдено"""
    for rank in ranks:
        if ranks.count(rank) == n:
            return rank
    return None


def two_pair(ranks):
    """Если есть две пары, то возврщает два соответствующих ранга,
    иначе возвращает None"""
    pair1 = kind(2, ranks)
    pair2 = kind(2, list(reversed(ranks)))
    if pair1 and pair2 and pair1 != pair2:
        return [pair1, pair2]
    return None


def best_hand(hand):
    """Из "руки" в 7 карт возвращает лучшую "руку" в 5 карт """
    return max(itertools.combinations(hand, 5), key=hand_rank)


# Вычисляем колоды заранее, чтобы не делать это при каждом вызове best_wild_hand
red_cards = [rank + suit for rank in '23456789TJQKA' for suit in 'HD']
black_cards = [rank + suit for rank in '23456789TJQKA' for suit in 'CS']


def best_wild_hand(hand):
    """best_hand но с джокерами"""
    simple_cards = [card for card in hand if card not in ['?B', '?R']]
    joker_cards = [card for card in hand if card in ['?B', '?R']]

    hands = [simple_cards]
    for joker_card in joker_cards:
        if joker_card == '?R':
            hands = [h + [c] for h in hands for c in red_cards if c not in simple_cards]
        elif joker_card == '?B':
            hands = [h + [c] for h in hands for c in black_cards if c not in simple_cards]

    combinations = []
    for hand in hands:
        combinations.extend(list(itertools.combinations(hand, 5)))

    return max(combinations, key=hand_rank)


def test_best_hand():
    print "test_best_hand..."
    assert (sorted(best_hand("6C 7C 8C 9C TC 5C JS".split()))
            == ['6C', '7C', '8C', '9C', 'TC'])
    assert (sorted(best_hand("TD TC TH 7C 7D 8C 8S".split()))
            == ['8C', '8S', 'TC', 'TD', 'TH'])
    assert (sorted(best_hand("JD TC TH 7C 7D 7S 7H".split()))
            == ['7C', '7D', '7H', '7S', 'JD'])
    assert (sorted(best_hand("TD TC TH 7C 7D 8C 8S".split()))
            == ['8C', '8S', 'TC', 'TD', 'TH'])
    print 'OK'


def test_best_wild_hand():
    print "test_best_wild_hand..."
    assert (sorted(best_wild_hand("6C 7C 8C 9C TC 5C ?B".split()))
            == ['7C', '8C', '9C', 'JC', 'TC'])
    assert (sorted(best_wild_hand("TD TC 5H 5C 7C ?R ?B".split()))
            == ['7C', 'TC', 'TD', 'TH', 'TS'])
    assert (sorted(best_wild_hand("JD TC TH 7C 7D 7S 7H".split()))
            == ['7C', '7D', '7H', '7S', 'JD'])
    print 'OK'


def test_card_ranks():
    print "test_card_ranks..."
    assert card_ranks("7C 6C 8C 9C TC".split()) == [8, 7, 6, 5, 4]
    assert card_ranks("JD TC TH 7C 5D".split()) == [9, 8, 8, 5, 3]
    assert card_ranks("JD QC QH KC AD".split()) == [12, 11, 10, 10, 9]
    print 'OK'


def test_flush():
    print "test_flush..."
    assert flush("7C 6C 8C 9C TC".split()) is True
    assert flush("JD TC TH 7C 5D".split()) is False
    assert flush("JD QC QH KC AD".split()) is False
    assert flush("JD QD KD AD 9D".split()) is True
    print 'OK'


def test_straight():
    print "test_straight..."
    assert straight(card_ranks("7C 6C 8C 9C TC".split())) is True
    assert straight(card_ranks("JD TC TH 7C 5D".split())) is False
    assert straight(card_ranks("JD QC QH KC AD".split())) is False
    assert straight(card_ranks("JD QD KD AD TD".split())) is True
    print 'OK'


def test_kind():
    print "test_kind..."
    assert kind(2, card_ranks("7C 6C 8C 9C TC".split())) is None
    assert kind(2, card_ranks("JD TC TH 7C 5D".split())) == 8
    assert kind(3, card_ranks("JD QC QH KC QD".split())) == 10
    assert kind(2, card_ranks("TD 9D 9H JH TC".split())) == 8
    print 'OK'


def test_two_pair():
    print "test_two_pair..."
    assert two_pair(card_ranks("7C 6C 8C 9C TC".split())) is None
    assert two_pair(card_ranks("TD 9D 9H JH TC".split())) == [8, 7]
    assert two_pair(card_ranks("KD QC QH KC QD".split())) is None
    print 'OK'


if __name__ == '__main__':
    test_best_hand()
    test_best_wild_hand()
    test_card_ranks()
    test_flush()
    test_straight()
    test_kind()
    test_two_pair()
