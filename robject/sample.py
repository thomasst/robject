import rm
import robject

robject.connect()

sorted = robject.Sorted('mykey')
print sorted.all()
print sorted.add("tom", 1)
print sorted.add("anna", 2)
print sorted.add("bob", 3)
print sorted.add("natalia", 4)
print sorted.add("john", 5)
print sorted.all()
print sorted.contains("bob")
print sorted.remove("bob")
print sorted.contains("bob")
print sorted.incr("tom", 10)
print sorted.all()
print sorted.count()
print sorted.delete()
print sorted.count()
print sorted.all()

s = robject.Set('myset')
s.add('a')
s.add('b')
print 'a' in s
print 'c' in s
print s.all()
print s.count()

s2 = robject.Set('myset2')
s.move('a', s2)

print s.all()
print s2.all()
