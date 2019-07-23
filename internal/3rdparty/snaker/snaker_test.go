package snaker

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Snaker", func() {

	Describe("SnakeToCamel test", func() {
		It("should return an empty string on an empty input", func() {
			Expect(SnakeToCamel("")).To(Equal(""))
		})

		It("should not blow up on trailing _", func() {
			Expect(SnakeToCamel("potato_")).To(Equal("Potato"))
		})

		It("should return a snaked text as camel case", func() {
			Expect(SnakeToCamel("this_has_to_be_uppercased")).To(
				Equal("ThisHasToBeUppercased"))
		})

		It("should return a snaked text as camel case, except the word ID", func() {
			Expect(SnakeToCamel("this_is_an_id")).To(Equal("ThisIsAnID"))
		})

		It("should return 'id' not as uppercase", func() {
			Expect(SnakeToCamel("this_is_an_identifier")).To(Equal("ThisIsAnIdentifier"))
		})

		It("should simply work with id", func() {
			Expect(SnakeToCamel("id")).To(Equal("ID"))
		})

		It("should work with initialism where only certain characters are uppercase", func() {
			Expect(SnakeToCamel("oauth_client")).To(Equal("OAuthClient"))
		})
	})
})
