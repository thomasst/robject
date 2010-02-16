import rm
import unittest

class TestModel(rm.Model):
    a = rm.Attribute()
    b = rm.Attribute()

class ModelTests(unittest.TestCase):
    def testCreateDelete(self):
        count = TestModel.objects.count()

        instance = TestModel(a='text a', b='text b')
        self.assertFalse(instance.id)

        instance.save()
        self.assertTrue(instance.id)
        self.assertTrue(TestModel.objects.contains(instance.id))

        new_count = TestModel.objects.count()
        self.assertEquals(count+1, new_count)

        new_instance = TestModel.get(id=instance.id)
        self.assertTrue(new_instance)
        self.assertEquals(new_instance.a, 'text a')
        self.assertEquals(new_instance.b, 'text b')

        new_instance.a = 'new a'
        new_instance.save()
        self.assertEquals(instance.id, new_instance.id)

        new_instance_2 = TestModel.get(id=instance.id)
        self.assertEquals(new_instance_2.a, 'new a')
        self.assertEquals(new_instance_2.b, 'text b')

        instance.delete()
        self.assertFalse(TestModel.get(id=instance.id))
        self.assertFalse(TestModel.objects.contains(instance.id))

        new_count = TestModel.objects.count()
        self.assertEquals(count, new_count)
    
        instance = TestModel.create(a='text a', b='text b')
        self.assertTrue(instance.id)
        instance.delete()

        new_count = TestModel.objects.count()
        self.assertEquals(count, new_count)

    def testSlicing(self):
        pass # TODO

if __name__ == "__main__":
    unittest.main()
