#!/usr/bin/python

import pdb

def wildcards(topic):
	"""Calculate topic wildcards
	"""
	sections = topic.split('/')
	length = len(sections)
	topics = []
	pre_level = ['']
	for level in range(length + 1):
		section = None
		if level < length:
			section = sections[level]
		is_last_section = False
		if level + 1 == length:
			is_last_section = True
		pre_level_ = []
		for s in pre_level:
			if section is not None:
				new_topic = s + '/' + section if len(s) > 0 else section
				if is_last_section:
					topics.append(new_topic)
					pre_level_.append(new_topic)
				else:
					pre_level_.append(new_topic)
			new_topic = s + '/' + '#' if len(s) > 0 else '#'
			topics.append(new_topic)
			new_topic = s + '/' + '+' if len(s) > 0 else '+'
			if is_last_section:
				topics.append(new_topic)
				pre_level_.append(new_topic)
			else:
				pre_level_.append(new_topic)
			pre_level = pre_level_
	return topics

def print_(topic):
	print 'Topic is "%s"' % topic
	topics = wildcards(topic)
	print 'Print result'
	index = 0
	for t in topics:
		print '%s: %s' % (index, t)
		index = index + 1



if __name__ == '__main__':
	pdb.set_trace()
	print_('/finance')
	#print_('apple')
	#print_('apple/orange')
	#print_('apple/orange/pear')
	#print_('apple/orange/pear/a')
	#print_('apple/orange/pear/a/b')
